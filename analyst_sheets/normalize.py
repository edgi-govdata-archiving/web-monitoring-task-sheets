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
        link_target = node.get('href')
        if link_target:
            node['href'] = normalize_url(link_target, base)

        # Don't consider the targets of links that are ARIA controls for other
        # elements on the page. This is iffy, but currently causing major
        # issues for some pages where the targets have autogenerated IDs, like
        # CDC's collapsible navigation:
        #   https://monitoring.envirodatagov.org/page/fbf2067c-b572-4e9d-b30e-b2afac6f2db4/6771ad62-cc7d-407e-b014-363ba19266e1..83d27b6c-c127-4873-ac42-153639c4ca62
        # We are optimistically presuming that the text of the link will change
        # if there's really something a change worth noting in this case.
        # NOTE: we should ideally come up with a solution in the links diff
        # that ignores cases like this (if we are confident it's generic
        # enough). It will have to not destroy the link like we are doing here.
        # This is only OK because it's being used for comparison rather than
        # actual analysis of the link.
        aria_target = node.get('aria-controls')
        if aria_target and (
            not link_target
            or link_target.startswith('#')
            or link_target.startswith('javascript:')
        ):
            node['href'] = '#'

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
    remove_selectors(soup, ('input[type="hidden"]', 'input[name^="__"]'))

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

    # For on-page URLs return just the fragment.
    # This helps keep comparisons reasonable -- the target didn't really change
    # even if the base_url changed between two documents we're looking at.
    if result.lower().startswith(f'{base_url.lower()}#'):
        return result[len(base_url):]

    # Use HTTPS for all web URLs. Don't translate other schemes (e.g. FTP).
    if result.startswith('http:'):
        result = f'https:{result[5:]}'

    result = remove_session_id(result)

    # TODO: Make URL relative to host in case of domain changes
    # (Most commonly www.xyz.gov -> xyz.gov, but could be others, too.)

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
    selectors = [
        # Social media share links
        '.follow-links',
        '.social-links',
        '[id*="social"]',
        '[id*="share"]',
        '[id*="sharing"]',
        '[class*="social"]',
        '[class*="share"]',
        '[class*="sharing"]',

        # Twitter feeds
        '[class*="twitter-feed"]',
        '[class*="twitter_feed"]',
        '[class*="tweet-feed"]',
        '[class*="tweet_feed"]',

        # Related/explore links, often at the bottom of pages.
        # This is tricky; we don't want to remove "related" resource links that
        # actually are part of/specific to the main content of the page.
        #
        # These are probably OK, but possible they could be too generic.
        # From http://climate.nasa.gov/climate_resources/*
        # ^ This specific case could be fixed by improving our readability
        #   fallback, too.
        '.carousel_teaser',
        '.multimedia_teaser',

        # Ignore FontAwesome icons.
        'i.fa',
    ]

    if not is_news_page(soup, url):
        selectors.extend((
            # Candidates that seem iffy, but may consider adding:
            #   `.latest-updates`
            '.news-section',
            '.news_content',
            '.news-content',
            '.box.news',
            '.panel.news',
            '.pane.news',
            '.panel-pane.news',
            '[class*="pane-news"]',
            '[class*="pane_news"]',
            '.news-feed',
            '.home-news-feed',
            '[class*="pane-blog"]',
            '.whats-new',
            '.press-room',
            '#nav-news',
            'nav .news',
            # This might be too specific? Maybe [class*="news-teaser"] is
            # better? Comes from:
            # https://dph.georgia.gov/health-topics/coronavirus-covid-19
            '.news-teaser-list',

            # NOT GENERALIZED. Specific to certain sites/pages.
            # Some of these may be ripe for promotion to the more general selector
            # above. Needs some thought.
            #
            # nrcc.cornell.edu
            # (ex: https://monitoring.envirodatagov.org/page/8d4dca19-e79e-467a-aa9a-5bb2df1a9eb3/ea61eb48-6816-4d1d-814e-100af9cf99f5..0311cad6-6427-422a-92e0-867c6c9fa5fc)
            'nav .nrcc-webinar-content',
            'nav [id*="blog-content"]',
            'nav [class*="blog-content"]',
            # doi.gov
            # (ex: https://monitoring.envirodatagov.org/page/c0926b52-9361-42ba-b6f5-516a4068dedf/9ed6ef6d-caff-4b57-a4b7-1cd26bf42332..dada2ab9-64ae-4408-aee0-b1794b17e438)
            # (Checking the container has "related-content" because I worry
            # this is *slightly* too broad otherwise.)
            '[class*="related-content"] [class*="press-release"]',
        ))

    # NOT GENERALIZED! These will all be very brittle, and we can't extend
    # these to cover everything. Only add for items known to cause real pain.
    # TODO: move these into a separate file so someone could swap in a
    # different URL-based list of things to remove.
    if 'defense.gov/' in url:
        selectors.append('.dgov-carousel-explore')
    elif 'globalchange.gov/' in url:
        selectors.append('aside [class*="related-reports"]')
    elif 'fema.gov/' in url:
        selectors.append('[class*="blockfeed-on-disaster-pages"]')

    # TODO: https://scenarios.globalchange.gov/regions/* has a list of images
    # that appears to be unordered or sorted on some criteria that changes
    # frequently. Not sure if we really want to block out that content, though.
    # e.g. https://monitoring.envirodatagov.org/page/7790790a-f46c-41fe-83da-bbe4bf850c9d/d1725a82-4cc7-4d9b-8ee6-673ab6facd54..fa2f99d7-cd70-4358-9044-4aabb82763e0

    # TODO: search/listing result sets? e.g:
    #   - `[class*="document-lister"]` on https://monitoring.envirodatagov.org/page/050d4127-aa89-4f4c-bf43-41bb3b5b66a9/71ca0829-afd3-4299-86bc-419e2a41e1f7..19f1ed78-e089-4930-923b-4b9d63d7aa61

    remove_selectors(soup, selectors)


def remove_selectors(soup, selectors):
    selector = ', '.join(selectors)
    for node in soup.select(selector):
        node.extract()


def is_news_page(soup, url):
    terms = ('news', 'press', 'blog')
    title = soup.title or ''
    if any(term in title for term in terms):
        return True
    if any(f'/{term}' in url for term in terms):
        return True

    return False
