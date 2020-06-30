"""
Experimental tools for normalizing HTML source before analysis. We might move
these lower down in the dependency stack later.
"""
import html5_parser
from surt import surt
from urllib.parse import urljoin, urlparse, urlunparse


def normalize_html(html, url):
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

    return soup.prettify()


def normalize_url(url, base_url):
    absolute = urljoin(base_url, url)
    # Use SURT to do most normalization, but don't return in SURT format.
    result = surt(absolute, reverse_ipaddr=False, surt=False, with_scheme=True)
    # Use HTTPS for all web URLs. Don't translate other schemes (e.g. FTP).
    if result.startswith('http:'):
        result = f'https:{result[5:]}'

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
