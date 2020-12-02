"""
Experimental tools for normalizing HTML source before analysis. We might move
these lower down in the dependency stack later.
"""
from bs4 import Comment, NavigableString
from bs4.formatter import EntitySubstitution, HTMLFormatter
from collections import defaultdict
from contextlib import contextmanager
import html5_parser
import re
import soupsieve
from surt import surt
from urllib.parse import parse_qs, urlencode, urljoin


INFIX_PUNCTUATION = re.compile(r'''['‘’]''')
NON_WORDS = re.compile(r'\W+')


def normalize_text(text):
    """
    Normalize a chunk of text from an HTML document by casefolding and removing
    punctuation.
    """
    # Remove punctuation between words
    return NON_WORDS.sub(
        ' ',
        # Remove punctuation that occurs inside words
        INFIX_PUNCTUATION.sub(
            '',
            # Lower-case and normalize unicode characters
            text.casefold()
        )
    )


def normalize_soup_text(text):
    """
    Normalize and format a BeautifulSoup text node by casefolding and removing
    punctuation.
    """
    # Only run this for text nodes, not attributes, comments, etc.
    if isinstance(text, NavigableString) and text.parent:
        text = normalize_text(text)
    return EntitySubstitution.substitute_html(text)


FORMATTER = HTMLFormatter(entity_substitution=normalize_soup_text)


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

    with optimized_soupsieve():
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

    return soup.prettify(formatter=FORMATTER)


def get_main_content(html):
    """
    Attempt to find the main content area of an HTML document and return a new
    document containing only that content. Returns `None` if not main content
    can be found.
    """
    # This is definitely naive right now, and only works with well-coded pages.
    # It's a start, though!
    # Some examples of pages this can't handle:
    #   https://www.blm.gov/nm/st/en/prog/energy.html
    # OK, but not great:
    #   https://www.cdc.gov/coronavirus/2019-ncov/index.html
    soup = html5_parser.parse(html, treebuilder='soup', return_root=False)

    # Some pages have empty bodies, and that's OK. Return a document since we
    # can declare the main content to be empty, rather than failing to find a
    # main content area.
    if soup.body.get_text().strip() == '':
        return html
    # Similarly, bodies that only have text and no elements are OK.
    elif all(isinstance(n, NavigableString) for n in soup.body.children):
        return html

    # These obviously only work for very nicely marked-up pages. Would be good
    # to eventually do better, but we also want to stay much more conservative
    # than, say, Mozilla Readability. For an example of why, see:
    #   github.com/edgi-govdata-archiving/web-monitoring-task-sheets/issues/9
    page_header = soup.find(role='banner') or soup.header
    if not page_header:
        page_header = soup.body.find(string=lambda c: (
            isinstance(c, Comment)
            and ('end head' in c.lower() or '/head' in c.lower())
        ))
    page_footer = soup.find(role='contentinfo')
    if not page_footer:
        footers = soup.find_all('footer')
        if footers and len(footers) > 0:
            page_footer = footers[-1]
    if not page_footer:
        page_footer = soup.body.find(string=lambda c: (
            isinstance(c, Comment)
            and ('begin foot' in c.lower() or 'start foot' in c.lower())
        ))
    main = (soup.main
            or soup.find(role='main')
            or soup.find(id='main'))
    if not main:
        # If only one <article>, it's probably the main content.
        articles = soup.find_all('article')
        if articles and len(articles) == 1:
            main = articles[0]
    # Making an assumption that the first <nav> is main/site-level navigation.
    nav = soup.find('nav') or soup.find(role='navigation')

    # If we couldn't find anything to demarcate useful parts of the page, bail
    # out. Return `None` instead of the page as-is so the caller knows.
    if not main and not page_header and not page_footer:
        return None

    # First, remove header + everything before and footer + everything after.
    # The header or footer *could* be in the main content block, so we need to
    # do this first.
    if page_header:
        remove_surroundings(page_header, before_only=True)
        page_header.extract()
    if page_footer:
        remove_surroundings(page_footer, after_only=True)
        page_footer.extract()
    if nav:
        remove_surroundings(nav, before_only=True)
        nav.extract()

    # Drop everything not in the main content area.
    if main:
        soup.body.clear()
        soup.body.append(main)

    return soup.prettify(formatter=FORMATTER)


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
    # NOTE: All selectors use "_" where "-" might also be present. We replace
    # "-" with "_" before matching. See `optimized_soupsieve()`.
    selectors = [
        # Social media share links
        '.follow_links',
        '.social_links',
        '[id*="social"]',
        '[id*="share"]',
        '[id*="sharing"]',
        '[class*="social"]',
        '[class*="share"]',
        '[class*="sharing"]',

        # Twitter feeds
        '[class*="twitter_feed"]',
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
            #   `.latest_updates`
            '.news_section',
            '.news_content',
            '.box.news',
            '.pane.news',
            '.panel.news',
            '.panel_pane.news',
            '[class*="pane_news"]',
            '.news_feed',
            '.home_news_feed',
            '[class*="pane_blog"]',
            '.whats_new',
            '.press_room',
            '#nav_news',
            'nav .news',
            # This might be too specific? Maybe [class*="news-teaser"] is
            # better? Comes from:
            # https://dph.georgia.gov/health-topics/coronavirus-covid-19
            '.news_teaser_list',

            # NOT GENERALIZED. Specific to certain sites/pages.
            # Some of these may be ripe for promotion to the more general
            # selector above. Needs some thought.
            #
            # nrcc.cornell.edu
            # (ex: https://monitoring.envirodatagov.org/page/8d4dca19-e79e-467a-aa9a-5bb2df1a9eb3/ea61eb48-6816-4d1d-814e-100af9cf99f5..0311cad6-6427-422a-92e0-867c6c9fa5fc)
            'nav .nrcc_webinar_content',
            'nav [id*="blog_content"]',
            'nav [class*="blog_content"]',
            # doi.gov
            # (ex: https://monitoring.envirodatagov.org/page/c0926b52-9361-42ba-b6f5-516a4068dedf/9ed6ef6d-caff-4b57-a4b7-1cd26bf42332..dada2ab9-64ae-4408-aee0-b1794b17e438)
            # Worried these may be slightly too broad. Should they be gated by
            # URL with the others below?
            '[class*="pane_related_content"]',
            '[class*="panel_related_content"]',
            '[class*="related_content_pane"]',
        ))

    # NOT GENERALIZED! These will all be very brittle, and we can't extend
    # these to cover everything. Only add for items known to cause real pain.
    # TODO: move these into a separate file so someone could swap in a
    # different URL-based list of things to remove.
    if 'defense.gov/' in url:
        selectors.append('.dgov_carousel_explore')
    elif 'globalchange.gov/' in url:
        selectors.append('aside [class*="related_reports"]')
    elif 'fema.gov/' in url:
        selectors.append('[class*="blockfeed_on_disaster_pages"]')

    # TODO: https://scenarios.globalchange.gov/regions/* has a list of images
    # that appears to be unordered or sorted on some criteria that changes
    # frequently. Not sure if we really want to block out that content, though.
    # e.g. https://monitoring.envirodatagov.org/page/7790790a-f46c-41fe-83da-bbe4bf850c9d/d1725a82-4cc7-4d9b-8ee6-673ab6facd54..fa2f99d7-cd70-4358-9044-4aabb82763e0

    # TODO: search/listing result sets? e.g:
    #   - `[class*="document_lister"]` on https://monitoring.envirodatagov.org/page/050d4127-aa89-4f4c-bf43-41bb3b5b66a9/71ca0829-afd3-4299-86bc-419e2a41e1f7..19f1ed78-e089-4930-923b-4b9d63d7aa61

    remove_selectors(soup.body, selectors)


def remove_selectors(soup, selectors):
    selector = ', '.join(selectors)
    for node in soup.select(selector):
        node.extract()


def remove_surroundings(target, before_only=False, after_only=False):
    """
    Remove elements and text before or after a given element.
    """
    parents = tuple(target.parents)
    removables = []
    if not after_only:
        removables.append(tuple(target.previous_elements))
    if not before_only:
        removables.append(tuple(target.next_elements))
    for stack in removables:
        for node in stack:
            if node.name == 'body':
                break
            elif node not in parents:
                node.extract()


def is_news_page(soup, url):
    terms = ('news', 'press', 'blog')
    title = soup.title or ''
    if any(term in title for term in terms):
        return True
    if any(f'/{term}' in url for term in terms):
        return True

    return False


# TODO: We know slow selector matching is a major bottleneck, so consider
# implementing a `SimpleSelector` that works for a narrow set of selector
# features and takes dumb shortcuts. (Features we use: classes, attributes,
# IDs (which are just attributes), tag names, and ancestors)
@contextmanager
def optimized_soupsieve():
    """
    Monkey-patch soupsieve (the module that supports `soup.select()`) to
    perform *slightly* faster (it still has a lot of built-in overhead). When
    this context manager ends, the original behavior is restored.

    It does a few things:
    - Rewrites classes and IDs to replace "-" with "_", since we are
      looking for pretty generic names.
    - Treats CSS classes as a set instead of a list for a minor perf boost.
    - Caches transformed attributes (from above pionts) for repeated lookups.
    - Assumes attributes are stored in lower-case (in HTML, attribute and
      element names are not case sensitive, and we use a parser that lower-
      cases them for us).
    """
    element_attr_cache = defaultdict(dict)

    @staticmethod
    def custom_get_attribute_by_name(el, name, default=None):
        """
        Get an attribute by name from a given element, but slightly optimized.
        for our case. This is designed to replace SoupSieve's internals at:
            soupsieve.css_match._DocumentNav.get_attribute_by_name
        Original source:
            https://github.com/facelessuser/soupsieve/blob/5dc926093d2052aaff00ce669980054c3624323a/soupsieve/css_match.py#L279-L294
        """
        # Cache by ID because the hash is mutable for soup elements. :(
        attr_cache = element_attr_cache[id(el)]
        if name not in attr_cache:
            value = el.attrs.get(name, default)
            if name == 'id':
                value = value.strip().lower().replace('-', '_')
            elif name == 'class':
                value = set(c.strip().lower().replace('-', '_') for c in value)
            attr_cache[name] = value
        return attr_cache[name]

    sieve_nav = soupsieve.css_match._DocumentNav
    _get_attribute_by_name = sieve_nav.get_attribute_by_name
    sieve_nav.get_attribute_by_name = custom_get_attribute_by_name

    try:
        yield
    finally:
        sieve_nav.get_attribute_by_name = _get_attribute_by_name
        element_attr_cache = None
