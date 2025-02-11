"""
Tools to analyze a page.
"""

import concurrent.futures
import multiprocessing
from .normalize import normalize_html, normalize_text, normalize_url, get_main_content
from surt import surt
import sys
from .tools import (CharacterToWordDiffs, changed_ngrams, load_url,
                    parallel, parse_html_readability, ActivityMonitor)
from .terms import KEY_TERMS, KEY_TERM_GRAMS
from toolz.itertoolz import concat
import threading
import traceback

from collections import Counter
from contextlib import contextmanager
import concurrent.futures
import functools
import hashlib
import json
import math
import os.path
import re
import sys
from urllib.parse import urlparse
from web_monitoring_diff import (html_source_diff, html_text_diff,
                                 links_diff_json)

import signal
from web_monitoring.utils import Signal

SKIP_READABILITY_URLS = frozenset((
    'cdc.gov/',
))

# Can Analyze? ----------------------------------------------------------------
# This script can only really handle HTML and HTML-like data.

REQUIRE_MEDIA_TYPE = False

# text/* media types are allowed, so only non-text types need be explicitly
# allowed and only text types need be explicitly disallowed.
ALLOWED_MEDIA = frozenset((
    # HTML should be text/html, but these are also common.
    'application/html',
    'application/xhtml',
    'application/xhtml+xml',
    'application/xml',
    'application/xml+html',
    'application/xml+xhtml'
))

DISALLOWED_MEDIA = frozenset((
    'text/calendar',
    'application/rss+xml'
))

# Extensions to check for if there's no available media type information.
DISALLOWED_EXTENSIONS = frozenset((
    '.jpg',
    '.pdf',
    '.athruz',
    '.avi',
    '.doc',
    '.docbook',
    '.docx',
    '.dsselect',
    '.eps',
    '.epub',
    '.exe',
    '.gif',
    '.jpeg',
    '.jpg',
    '.kmz',
    '.m2t',
    '.mov',
    '.mp3',
    '.mpg',
    '.pdf',
    '.png',
    '.ppt',
    '.pptx',
    '.radar',
    '.rtf',
    '.wmv',
    '.xls',
    '.xlsm',
    '.xlsx',
    '.xml',
    '.zip'
))


def is_fetchable(url):
    return url and (url.startswith('http:') or url.startswith('https:'))


def is_allowed_extension(url):
    extension = os.path.splitext(urlparse(url).path)[1]
    return not extension or extension not in DISALLOWED_EXTENSIONS


def is_analyzable_media(version):
    media = version['media_type']
    if media:
        return media in ALLOWED_MEDIA or (
            media.startswith('text/') and media not in DISALLOWED_MEDIA)
    elif not REQUIRE_MEDIA_TYPE:
        return is_allowed_extension(version['url'])
    else:
        return False


class AnalyzableError(ValueError):
    ...


class NoChangeError(AnalyzableError):
    ...


def assert_can_analyze(page):
    total_versions = len(page['versions'])
    if total_versions < 2:
        raise NoChangeError('Page has only one version')

    a = page['versions'][-1]
    b = page['versions'][0]

    if a['body_hash'] == b['body_hash']:
        raise NoChangeError('First and last versions were exactly the same')

    if not is_fetchable(a['body_url']) or not is_fetchable(b['body_url']):
        raise AnalyzableError('Raw response data for page is not retrievable')

    if not is_analyzable_media(a) or not is_analyzable_media(b):
        raise AnalyzableError('Media types of versions cannot be analyzed')

    return True


# Analysis! -------------------------------------------------------------------

def calculate_percent_changed(diff):
    total_size = 0
    changed_size = 0
    for operation, text in diff:
        total_size += len(text)
        if operation != 0:
            changed_size += len(text)

    if total_size == 0:
        return 0.0

    # XXX: DEMO ONLY! Shortening should be part of output formatting.
    return round(changed_size / total_size, 4)
    return changed_size / total_size


def analyze_text(page, a, b, use_readability=True):
    # Check whether our readability fallback would work.
    # We always do this (rather than only as a fallback) so we can debug issues
    # with it by reporting any URLs that would have failed.
    text_a = a['normalized']
    text_b = b['normalized']
    content_a, readable_a = get_main_content(text_a)
    content_b, readable_b = get_main_content(text_b)
    found_content_area = bool(content_a and content_b)

    readable = False
    if (
        use_readability and
        not any(item in page['url'] for item in SKIP_READABILITY_URLS)
    ):
        with ActivityMonitor(f'load readable content for {page["uuid"]}'):
            response_a, response_b = parallel((parse_html_readability, a['response'].text, a['url']),
                                              (parse_html_readability, b['response'].text, b['url']))
        # parse_html_readability returns None if the content couldn't be parsed by
        # readability. If either one of the original documents couldn't be parsed,
        # fall back to straight HTML text for *both* (we want what we're diffing to
        # conceptually match up).
        if response_a and response_b:
            readable = True
            text_a = '\n'.join(normalize_text(line) for line in response_a.text.split('\n'))
            text_b = '\n'.join(normalize_text(line) for line in response_b.text.split('\n'))
            raw_diff = html_source_diff(text_a, text_b)

    if not readable:
        # Try using our own content detection as a fallback from readability.
        if content_a and content_b:
            text_a, text_b = content_a, content_b
            if readable_a and readable_b:
                readable = 'fallback-yes'
            else:
                readable = 'fallback-no'
        raw_diff = html_text_diff(text_a, text_b)

    diff = raw_diff['diff']
    diff_changes = [item for item in diff if item[0] != 0]

    # This script leverages our textual diffing routines, which work
    # character-by-character, so we have to recompose the results into *words*.
    # That's a little absurd, and we might be better off in the future to
    # tokenize the words and diff them directly instead.
    word_diff = CharacterToWordDiffs.word_diffs(raw_diff['diff'])

    # Count the terms that were added and removed.
    grams = KEY_TERM_GRAMS
    terms = (Counter(), Counter(),)
    for gram in range(1, grams + 1):
        terms[0].update(Counter(changed_ngrams(word_diff[0], gram)))
        terms[1].update(Counter(changed_ngrams(word_diff[1], gram)))

    all_terms = Counter(terms[1])
    all_terms.subtract(terms[0])
    key_terms = {term: all_terms[term]
                 for term in KEY_TERMS
                 if abs(all_terms.get(term, 0)) > 0}
    key_terms_changed = len(key_terms) > 0
    key_terms_change_count = sum((abs(count)
                                  for term, count in key_terms.items()))

    longest_change = 0
    if diff_changes:
        longest_change = max(len(text) for code, text in diff_changes)
    # Possible alternate take: the maximum diff *line* length. This would
    # prevent contiguous list items or paragraphs from bulking up the "longest"
    # change size. Not sure whether that's really good or bad, though.
    # for code, text in diff_changes:
    #     line_lengths = [len(line.strip()) for line in text.split('\n')]
    #     longest_change = max(longest_change, *line_lengths)

    return {
        'readable': readable,
        'found_content_area': found_content_area,
        'key_terms': key_terms,
        'key_terms_changed': key_terms_changed,
        'key_terms_change_count': key_terms_change_count,
        'percent_changed': calculate_percent_changed(raw_diff['diff']),
        # 'percent_changed_words': calculate_percent_changed(word_diff),
        'diff_hash': hash_changes(diff_changes),
        'diff_count': len(diff_changes),
        'diff_length': sum(len(text) for code, text in diff_changes),
        'diff_max_length': longest_change,
    }


def analyze_links(a, b):
    diff = links_diff_json(a['normalized'], b['normalized'])['diff']
    diff_changes = [item for item in diff if item[0] != 0]

    a_url = normalize_url(a['url'], a['url'])
    b_url = normalize_url(b['url'], b['url'])
    removed_self_link = any(link['href'] == a_url or link['href'] == b_url
                            for change, link in diff
                            if change == -1)

    # TODO: differentiate fragment vs. external links and treat differently?
    return dict(
        diff_hash=hash_changes(diff_changes),
        diff_length=len(diff_changes),
        diff_ratio=calculate_percent_changed(diff),
        removed_self_link=removed_self_link
    )


def analyze_source(a, b):
    # EXPERIMENT: use normalized HTML for analysis.
    # diff = html_source_diff(a['response'].text, b['response'].text)['diff']
    diff = html_source_diff(a['normalized'], b['normalized'])['diff']
    diff_changes = [item for item in diff if item[0] != 0]
    return dict(
        diff_hash=hash_changes(diff_changes),
        diff_count=len(diff_changes),
        diff_length=sum((len(text) for code, text in diff_changes)),
        diff_ratio=calculate_percent_changed(diff)
    )


def hash_changes(diff):
    if len(diff) > 0:
        diff_bytes = json.dumps(diff).encode('utf-8')
    else:
        diff_bytes = b''
    return hashlib.sha256(diff_bytes).hexdigest()


# NOTE: Ideally, we'd have info from DB about when the status changed. The
# below method could be inaccurate if `a` was a spurious error.
# See: https://github.com/edgi-govdata-archiving/web-monitoring-db/issues/630
def page_status_changed(page, a, b):
    first_ok = a['status'] >= 400
    page_ok = page.get('status', b['status']) >= 400
    # Hack to calculate the effective status over this time period because of
    # lots of known-bad captures from Wayback (so page['status'] is wrong)
    # FIXME: should probably reproduce the "effective status" algorithm here.
    if 'globalchange.gov/' in page['url']:
        page_ok = b['status'] >= 400
    return page_ok != first_ok


def status_code_type(status):
    if status is None or status < 400:
        return None
    elif status < 500:
        return 'user'
    else:
        return 'server'


def page_status_factor(page, a, b):
    """
    Get a priority factor based on the status codes seen over the timeframe.
    The basic goal here is to deprioritize changes on error pages, since they
    are unlikely to actually be meaningful. If the start and end versions were
    both errors, deprioritize a bit, and if they were the same type of error
    (e.g. both were 4xx errors), deprioritize a lot.
    """
    a_error = status_code_type(a['status'])
    b_error = status_code_type(b['status'])
    if a_error is None or b_error is None:
        return 1
    elif a_error == b_error:
        return 0.15
    else:
        return 0.4


# NOTE: this function is not currently used. For it to be reasonably effective,
# we need a way to tell whether two HTML documents are *practically* the same,
# not just exactly the same (via hash, which is what we have been doing). For
# more on why using this without that capability is harmful rather than
# helpful, see:
# https://github.com/edgi-govdata-archiving/web-monitoring-task-sheets/issues/2
def analyze_change_count(page, after, before):
    """
    Determine a factor for the number of changes that occurred during the time
    period. Up to a certain point, more changes might mean a page is more worth
    looking at. Past that point, the factor drops (i.e. there's probably just a
    lot of pointless churn on the page).

    The result maxes out at 1, though it can go below 0. See `min_factor`.
    """
    # Changes/day at which we factor most highly.
    max_rate = 0.6
    # Changes/day above which we factor negatively.
    max_positive_rate = 0.8
    # Minimum factor to return
    min_factor = -1

    # versions_count = len(page['versions'])
    versions_count = page['versions_count']
    first = page['versions'][-1]
    if first['capture_time'] < after:
        versions_count -= 1

    changes_per_day = versions_count / (before - after).days

    if changes_per_day <= max_rate:
        factor = changes_per_day / max_rate
    else:
        factor = 1 - (changes_per_day - max_rate) / (max_positive_rate - max_rate)
        factor = max(factor, min_factor)

    return factor


META_REFRESH_PATTERN = re.compile(r'<meta[^>]+http-equiv="refresh"[^>]*')
META_REFRESH_URL_PATTERN = re.compile(r'content="\d+(\.\d*)?;\s*url=([^"]+)"')


def get_client_redirect(version):
    match = META_REFRESH_PATTERN.search(version['response'].text)
    if match:
        url_match = META_REFRESH_URL_PATTERN.search(match.group(0))
        if url_match:
            return url_match.group(2)

    return None


def get_redirects(version):
    server = version['source_metadata'].get('redirects') or []
    if not isinstance(server, (list, tuple)):
        raise ValueError(f'Unknown type for redirects on version: {version}')

    client = get_client_redirect(version)
    combined = server + ([client] if client else [])
    return combined, server, client


def analyze_redirects(page, a, b):
    a_all, a_server, a_client = get_redirects(a)
    b_all, b_server, b_client = get_redirects(b)

    is_client_redirect = ''
    if a_client and b_client:
        is_client_redirect = True
    elif a_client and not b_client:
        is_client_redirect = 'was redirect'
    elif not a_client and b_client:
        is_client_redirect = 'became redirect'

    final_a = surt(a_all[-1] if len(a_all) else a['url'], reverse_ipaddr=False)
    final_b = surt(b_all[-1] if len(b_all) else b['url'], reverse_ipaddr=False)

    return {
        'changed': final_a != final_b,
        'client_changed': a_client != b_client,
        'is_client_redirect': is_client_redirect,
        'a_server': a_server,
        'b_server': b_server,
        'a_client': a_client,
        'b_client': b_client,
    }


ROOT_PAGE_PATTERN = re.compile(r'^/(index(\.\w+)?)?$')


def is_home_page(page):
    url_path = urlparse(page['url']).path
    return True if ROOT_PAGE_PATTERN.match(url_path) else False


# Calculate a multiplier for priority based on a ratio representing the amount
# of change. This is basically applying a logorithmic curve to the ratio.
def priority_factor(ratio):
    return math.log(1 + (math.e - 1) * ratio)


def analyze_page(page, after, before, use_readability=True):
    """
    Analyze a page from web-monitoring-db and return information about how the
    words on it changed between the first and latest captured versions.
    """
    assert_can_analyze(page)
    priority = 0  # 0 = "not worth human eyes", 1 = "really needs a look."
    baseline = 0  # Minimum priority. Some factors add to the baseline.

    versions_count = len(page['versions'])
    root_page = is_home_page(page)

    a = page['versions'][len(page['versions']) - 1]
    b = page['versions'][0]
    with ActivityMonitor(f'load raw content for {page["uuid"]}'):
        a['response'], b['response'] = parallel((load_url, a['body_url']),
                                                (load_url, b['body_url']))
    a['normalized'] = normalize_html(a['response'].text, a['url'])
    b['normalized'] = normalize_html(b['response'].text, b['url'])

    link_analysis = analyze_links(a, b)
    if link_analysis['diff_length'] > 0:
        baseline = max(baseline, 0.1)
        priority += 0.3 * priority_factor(link_analysis['diff_ratio'])
    # This most likely indicates a page was removed from navigation! Big deal.
    if link_analysis['removed_self_link']:
        priority += 0.75

    text_analysis = analyze_text(page, a, b, use_readability)
    if text_analysis['key_terms_changed']:
        priority += min(0.4, 0.05 * text_analysis['key_terms_change_count'])
    if text_analysis['diff_count'] > 0:
        priority += 0.65 * priority_factor(text_analysis['percent_changed'])
    # Increase the score if a single change is > 100 characters. The added
    # score increases up until 600 characters.
    if text_analysis['diff_max_length'] > 100:
        long_change_size = min(text_analysis['diff_max_length'], 600) - 100
        priority += 0.35 * long_change_size / 500

    # Ensure a minimum priority of both text and links changed.
    # This should probably stay less than 0.15.
    if text_analysis['diff_count'] > 0 and link_analysis['diff_length'] > 0:
        baseline = max(baseline, 0.125)

    source_analysis = analyze_source(a, b)

    status_changed = page_status_changed(page, a, b)
    if status_changed:
        priority += 1
    else:
        priority *= page_status_factor(page, a, b)

    redirect_analysis = analyze_redirects(page, a, b)
    if redirect_analysis['changed']:
        priority += 0.25

    # Ensure priority at least matches baseline.
    priority = max(priority, baseline)

    # Demote root pages, since they usually are just listings of other things.
    if root_page:
        priority *= 0.25

    # TODO: Demote topic pages?
    # 1 (or 2?) level deep and not readable

    # TODO: Demote news pages...?
    # if is_news_page(page['url']):
    #     priority *= 0.25

    return dict(
        priority=max(min(priority, 1), 0),
        versions=versions_count,
        root_page=root_page,
        status=page.get('status', b['status']),
        status_changed=status_changed,
        view_url=f'https://monitoring.envirodatagov.org/page/{page["uuid"]}/{a["uuid"]}..{b["uuid"]}',
        links=link_analysis,
        source=source_analysis,
        text=text_analysis,
        redirect=redirect_analysis,
    )


def work_page(after, before, use_readability, page):
    """
    In-process wrapper for analyze_page() that handles exceptions because
    Python multiprocessing seems to have issues with actual raised exceptions.
    """
    try:
        with ActivityMonitor(f'analyze {page["uuid"]}', alert_after=30):
            result = analyze_page(page, after, before, use_readability)
            return (page, result, None)
    except Exception as error:
        if not isinstance(error, AnalyzableError):
            # Format/serialize the traceback so it can cross process boundaries
            error.traceback = traceback.format_tb(error.__traceback__)
        # TODO: add option for more detailed logging
        return (page, None, error)


def ignore_signal(signal_type, frame):
    ...


def setup_worker():
    # Ignore sigint because the main process is handling it.
    # Total abuse of the context manager protocol :\
    handler = Signal((signal.SIGINT,), ignore_signal)
    handler.__enter__()


def analyze_pages(pages, after, before, use_readability=True, parallel=None, cancel=None):
    """
    Analyze a set of pages in parallel across multiple processes. Yields tuples
    for each page with:
    0. The page
    1. The analysis (or `None` if analysis failed)
    2. An exception if analysis failed or `None` if it succeeded. This will be
       an instance of `AnalyzableError` if the page or versions were not of a
       type that this module can actually analyze.
    """
    parallel = parallel or multiprocessing.cpu_count()
    # Python 3.8 does sets start method by platform like this by default. This
    # is just backporting that behavior. (Fork seems to occasionally cause real
    # issues with threading on MacOS.)
    method = sys.platform == 'darwin' and 'spawn' or 'fork'
    context = multiprocessing.get_context(method)
    with context.Pool(parallel, setup_worker, maxtasksperchild=100) as pool:
        if cancel:
            close_on_event(pool, cancel)

        work = functools.partial(work_page, after, before, use_readability)
        yield from pool.imap_unordered(work, pages)
        pool.close()


def close_on_event(pool, event):
    def wait_and_close():
        event.wait()
        pool.close()

    thread = threading.Thread(target=wait_and_close, daemon=True)
    thread.start()
    return thread
