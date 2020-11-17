"""
Experimental tools for normalizing HTML source before analysis. We might move
these lower down in the dependency stack later.
"""
import html5_parser
import re
from surt import surt
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse


def normalize_html(html, url, remove_extra_content=True):
    """
    Remove parts of the HTML that we expect to change pointlessly and result in
    two pages that are effectively the same still appearing different, and thus
    giving us higher analysis scores than we should have.
    Things like:
    - Meaningless link URL changes (absolute vs. relative, http vs. https,
      adding/removing slashes at the end)
    - Hashes on script/stylesheet URLs
    - Whitespace issues
    - ASP.net postback form fields
    - etc.
    """
    soup = html5_parser.parse(html, treebuilder='soup', return_root=False)
    base = get_base_url(soup, url)

    # Update Link URLs
    for node in soup.select('a'):
        if node.get('href'):
            node['href'] = normalize_url(node['href'], base)

    # Update media URLs
    for node in soup.select('img, source'):
        del node['srcset']
        if node.get('src'):
            node['src'] = normalize_url(node['src'], base)

    # Remove scripts and styles, metadata
    for node in soup.select('script, style, link, meta'):
        if node.name != 'meta' or node.get('charset') is None:
            node.extract()

    # Hidden form fields for postbacks or other session info
    for node in soup.select('input[type="hidden"], input[name^="__"]'):
        node.extract()

    # Attempt to drop news boxes, asides not related to content, etc.
    if remove_extra_content:
        remove_extraneous_nodes(soup, url)

    return soup.prettify()


def normalize_url(url, base_url):
    try:
        absolute = urljoin(base_url, url)
        # Use SURT to do most normalization, but don't return in SURT format.
        result = surt(absolute, reverse_ipaddr=False, surt=False,
                      with_scheme=True)
    except ValueError:
        # If the source was malformed or otherwise an invalid URL, just return
        # the original value for comparison.
        # TODO: have some kind of verbose flag that causes us to log this?
        return url

    # Use HTTPS for all web URLs. Don't translate other schemes (e.g. FTP).
    if result.startswith('http:'):
        result = f'https:{result[5:]}'

    result = remove_session_id(result)
    return result


def get_base_url(soup, url):
    """
    Get the base URL for a BeautifulSoup page, given the URL it was loaded
    from. This can then be used with ``urllib.parse.urljoin`` to make sure any
    link or resource URL on the page to get the absolute URL it refers to.

    Parameters
    ----------
    soup : BeautifulSoup
        A BeautifulSoup-parsed web page.
    url : str
        The URL that the web page was loaded from.

    Returns
    -------
    str
    """
    base = soup.find('base')
    if base and base['href']:
        return urljoin(url, base['href'].strip())
    else:
        return url


SERVLET_SESSION_ID = re.compile(r';jsessionid=[a-zA-Z0-9\-_,]+')
ASP_SESSION_ID = re.compile(r'/\(S\([a-zA-Z0-9\-_,]+\)\)/')


def remove_session_id(url):
    """
    Remove session IDs from URLs when comparing. Some servers, like Java
    Servlets, may store session IDs in the URL instead of cookies. We don't
    want to count those for URL comparisons, since they will almost always be
    different, even if the meaningful part of the URL is the same. For example,
    these two URLs would be equivalent:
    - https://www.ncdc.noaa.gov/homr/api;jsessionid=A2DECB66D2648BFED11FC721FC3043A1
    - https://www.ncdc.noaa.gov/homr/api;jsessionid=B3EFDC88E3759CGFE22GD832GD4154B2

    Because they both refer to `https://www.ncdc.noaa.gov/homr/api`.
    """
    # Java servlet and ASP.net session IDs can be anywhere in the URL.
    clean = SERVLET_SESSION_ID.sub('', url, count=1)
    clean = ASP_SESSION_ID.sub('/', clean, count=1)

    # Most others we know of are in the querystring.
    base, _, querystring = clean.partition('?')
    try:
        query = parse_qs(querystring, keep_blank_values=True)
    except ValueError:
        return clean
    query.pop('PHPSESSID', None)
    query.pop('phpsessid', None)
    query.pop('SID', None)
    query.pop('sid', None)
    query.pop('SESSIONID', None)
    query.pop('sessionid', None)
    query.pop('SESSION_ID', None)
    query.pop('session_id', None)
    query.pop('SESSION', None)
    query.pop('session', None)
    clean_querystring = urlencode(query, doseq=True)
    clean = f'{base}?{clean_querystring}' if clean_querystring else base

    return clean


def remove_extraneous_nodes(soup, url):
    """
    Attempt to find and remove content not primarily related to the page, like
    news boxes, ads, etc.
    """
    # Social media share links
    for node in soup.select('.follow-links, .social-links, [id*="social"], [id*="share"], [id*="sharing"], [class*="social"], [class*="share"], [class*="sharing"]'):
        node.extract()

    if not is_news_page(soup, url):
        # Candidates that seem iffy, but may consider adding:
        #   .latest-updates
        for node in soup.select('.box.news, .panel.news, .pane.news, .panel-pane.news, .news-feed, .home-news-feed, [class*="pane-blog"]'):
            node.extract()

    for node in soup.select('.twitter-feed'):
        node.extract()


def is_news_page(soup, url):
    terms = ('news', 'press', 'blog')
    title = soup.title or ''
    if any(term in title for term in terms):
        return True
    if any(f'/{term}' in url for term in terms):
        return True

    return False
