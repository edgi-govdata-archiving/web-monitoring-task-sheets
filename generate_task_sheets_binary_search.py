from analyst_sheets import analyze
# from analyst_sheets.sheets import write_csv
from analyst_sheets.tools import (
    generate_on_thread,
    map_parallel,
    QuitSignal,
    ActivityMonitor,
    get_thread_db_client,
    tap
)
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import dateutil.parser
from dateutil.tz import tzutc
import gzip
from itertools import islice
import json
from pathlib import Path
from retry import retry
import signal
import sys
from typing import Iterable, TypeAlias
from tqdm import tqdm
import threading
import traceback
from web_monitoring import db


ResultItem: TypeAlias = tuple[dict, dict | None, Exception | None]


def list_all_pages(url_pattern, after, before, tags=None, cancel=None, client=None, total=False):
    client = client or get_thread_db_client()

    pages = client.get_pages(url=url_pattern,
                             tags=tags,
                             active=True,
                             include_total=total,
                             # start_date=after, end_date=before,
                             # include_* is very problematic; don't use it.
                             # (See https://github.com/edgi-govdata-archiving/web-monitoring-db/issues/858)
                             # include_earliest=True,
                             sort=['created_at:asc'],
                             chunk_size=500)

    # Handle canceling and emit total as first item
    yielded_total = total is False
    for page in pages:
        if not yielded_total:
            yield page['_list_meta']['total_results']
            yielded_total = True
        yield page
        if cancel and cancel.is_set():
            return


def list_page_versions(page_id, after, before, chunk_size=1000, cancel=None,
                       client=None):
    client = client or get_thread_db_client()

    if cancel and cancel.is_set():
        return

    versions = client.get_versions(page_id=page_id, different=False,
                                   start_date=after, end_date=before,
                                   # include_change_from_previous=True,
                                   # include_change_from_earliest=True,
                                   sort=['capture_time:desc'],
                                   chunk_size=chunk_size)

    for version in versions:
        yield version
        if cancel and cancel.is_set():
            return

    # Get version leading into timeframe.
    # TODO: this has been defunct for years; we should probably clean this up
    # and remove it.
    if after:
        raise RuntimeError('THIS LOGIC IS DEFUNCT!')
        yield next(client.get_versions(page_id=page_id,
                                       start_date=None, end_date=after,
                                       sort=['capture_time:desc'],
                                       chunk_size=1))


def maybe_bad_capture(version) -> bool:
    """
    Identify captures that are likely to have been blocked responses (e.g.
    a rate limit or firewall rule blocked the crawler) or intermittent errors.
    These don't represent what a regular user should have seen at the time, so
    we should avoid using them as candidates for comparison.
    """
    headers = {k.lower(): v for k, v in (version['headers'] or {}).items()}
    content_length = version['content_length']
    if content_length is None:
        content_length = int(headers.get('content-length', '-1'))

    status = version['status'] or 200
    if status == 200 and content_length == 0:
        status = 500

    if status < 400:
        return False

    server = headers.get('server', '').lower()

    no_cache = False
    if 'cache-control' in headers:
        cache_control = headers['cache-control'].lower()
        if 'no-cache' in cache_control:
            no_cache = True
        elif 'max-age=0' in cache_control:
            no_cache = True
    if no_cache is False and 'expires' in headers:
        expires = dateutil.parser.parse(headers['expires'])
        request_time = (
            dateutil.parser.parse(headers['date'])
            if 'date' in headers
            else version['capture_time']
        )
        no_cache = (expires - request_time).total_seconds() < 60

    x_cache = headers.get('x-cache', '').lower()
    cache_error = 'error' in x_cache or 'n/a' in x_cache

    is_short_or_unknown = content_length < 1000
    content_type = version['media_type'] or headers.get('content-type', '')
    is_html = content_type.startswith('text/html')

    if server.startswith('awselb/') and is_short_or_unknown and is_html:
        return True
    elif server == 'akamaighost' and is_short_or_unknown and no_cache:
        return True
    elif server == 'cloudfront':
        # TODO: Keeping these branches separate b/c the challenge response is
        # *definitely* a bad capture, while the cache_error is only probably.
        # In the future, we may refactor this function to return a float
        # indicating probable badness instead of a boolean.
        if headers.get('x-amzn-waf-action', '').lower() == 'challenge':
            return True
        elif cache_error:
            return True
    elif server == 'cloudflare':
        if headers.get('cf-mitigated', '').lower() == 'challenge':
            return True
    # TODO: see if we have any Azure CDN examples?
    # TODO: More general heuristics?
    # else:
    #     content_type = version['media_type'] or headers.get('content-type', '')
    #     x_cache = headers.get('x-cache', '').lower()
    #     cache_miss = x_cache and not x_cache.startswith('hit')
    #     return content_type.startswith('text/html') and is_short_or_unknown and cache_miss
    return False


def status_error_class(status: int | None) -> int:
    status_class = int((status or 600) / 100)
    return status_class if status_class >= 4 else 0


def trim_version_range(versions: list[dict], start_only: bool = False) -> list[dict]:
    """
    Trim same-ish versions from the beginning and end of a list of versions.
    Versions are considered the same if they have the same body hash or if they
    are HTTP errors of the same class (4xx, 5xx).

    The goal here is to make sure we are analyzing and presenting to users the
    narrowest time window we can for relevant changes.

    Ideally we'd do a more detailed pass after this that looks at the parsed,
    normalized content and discards stuff we don't care about like most
    ``<meta>`` or ``<script>`` tags, ``data-*`` attributes, etc., but this gets
    us pretty far.
    """
    if len(versions) < 3:
        return versions

    start_index = 0
    start_hash = versions[0].get('body_hash')
    start_status = status_error_class(versions[0].get('status'))
    for index, version in enumerate(versions):
        if start_status:
            if start_status != status_error_class(version.get('status')):
                break
        else:
            if start_hash != version.get('body_hash'):
                break
        start_index = index

    result = versions[start_index:]
    if start_only:
        return result

    return list(reversed(trim_version_range(
        list(reversed(result)),
        start_only=True
    )))


def add_versions_to_page(page, after, before, candidates):
    """
    Find all the relevant versions of a page in the given timeframe and attach
    them to the page object as 'versions'.
    """
    def in_time_range(version) -> bool:
        return version['capture_time'] >= after and version['capture_time'] < before

    all_versions = list_page_versions(page['uuid'], None, before, chunk_size=20)
    versions = []
    questionable_versions = []
    for version in all_versions:
        if in_time_range(version):
            if maybe_bad_capture(version):
                questionable_versions.append(version)
            else:
                versions.append(version)
        else:
            if len(versions) == 0:
                versions.extend(questionable_versions)
                if len(versions) == 0:
                    # There are no valid versions in the timeframe to analyze!
                    break

            # Look back a few more versions and up to N days for a valid
            # baseline version.
            baseline = version
            if maybe_bad_capture(baseline):
                for candidate in islice(all_versions, 2):
                    if (baseline['capture_time'] - candidate['capture_time']).days > 30:
                        break
                    elif not maybe_bad_capture(candidate):
                        baseline = candidate
                        break

            versions.append(baseline)
            break

    page['versions'] = trim_version_range(versions)
    return page


@dataclass
class PageAnalysisResult:
    id: str
    page: dict
    timeframe: list[dict]
    overall: dict | None = None
    changes: list[dict] = field(default_factory=list)
    error: Exception | None = None


def analyze_page(after: datetime, before: datetime, use_readability: bool, threshold: float, page: dict) -> PageAnalysisResult:
    """
    Search for and analyze the relevant changes in a page.
    """
    candidate_versions = list_page_versions(page['uuid'], None, before, chunk_size=20)
    full_period = add_versions_to_page(page.copy(), after, before, candidate_versions)

    result = PageAnalysisResult(
        id=page['uuid'],
        page=page,
        timeframe=[full_period['versions'][0], full_period['versions'][-1]],
    )
    _, overall, error = analyze.work_page(after, before, use_readability, full_period)
    if error:
        result.error = error
    else:
        result.overall = overall
        if overall['priority'] >= threshold:
            try:
                result.changes = find_relevant_changes(page, full_period['versions'], use_readability, threshold)
            except Exception as error:
                result.error = error

    return result


INDENT = '    '

def find_relevant_changes(page: dict, versions: list[dict], use_readability: bool, threshold: float, depth: int = 0) -> list[dict]:
    threshold = max(0.2, threshold)

    if len(versions) < 3:
        return []

    results = []
    split_at = len(versions) // 2
    periods = [versions[0:split_at + 1], versions[split_at:]]
    # print(
    #     f"{INDENT * depth}Splitting {page['uuid']} {versions[-1]['capture_time']} to "
    #     f"{versions[0]['capture_time']} ({len(periods[1])}, {len(periods[0])})"
    # )
    for period in reversed(periods):
        # indent = INDENT * (depth + 1)
        period_page = page.copy()
        period_page['versions'] = period
        # print(f"{indent}Analyzing {page['uuid']} {period[-1]['capture_time']} to {period[0]['capture_time']}")
        period_after = period[-1]['capture_time'] + timedelta(minutes=1)
        period_before = period[0]['capture_time']
        _, period_result, error = analyze.work_page(period_after, period_before, use_readability, period_page)
        if isinstance(error, analyze.NoChangeError):
            # print(f"{indent}(No change)")
            ...
        elif error:
            # print(f'{indent}UHOH: {error}')
            raise error
        elif period_result['priority'] >= threshold:
            subchanges = find_relevant_changes(page, period, use_readability, threshold, depth + 2)
            # print(f"{indent}{len(subchanges)} Subchanges")
            if subchanges:
                results.extend(subchanges)
            else:
                change = dict(versions=[period[0], period[-1]], analysis=period_result)
                results.append(change)
        else:
            # print(f"{indent}(Below threshold)")
            ...

    return results


from analyst_sheets.tools import Signal
import functools
import multiprocessing


def ignore_signal(signal_type, frame):
    ...


def setup_worker():
    # Ignore sigint because the main process is handling it.
    # Total abuse of the context manager protocol :\
    handler = Signal((signal.SIGINT,), ignore_signal)
    handler.__enter__()


def analyze_pages(pages, after, before, use_readability=True, threshold=0.25, cancel=None):
    """
    Analyze a set of pages in parallel across multiple processes. Yields tuples
    for each page with:
    0. The page
    1. The analysis (or `None` if analysis failed)
    2. An exception if analysis failed or `None` if it succeeded. This will be
       an instance of `AnalyzableError` if the page or versions were not of a
       type that this module can actually analyze.
    """
    with multiprocessing.Pool(initializer=setup_worker, maxtasksperchild=100) as pool:
        if cancel:
            close_on_event(pool, cancel)

        work = functools.partial(analyze_page, after, before, use_readability, threshold)
        yield from pool.imap_unordered(work, pages)
        pool.close()


def close_on_event(pool, event):
    def wait_and_close():
        event.wait()
        pool.close()

    thread = threading.Thread(target=wait_and_close, daemon=True)
    thread.start()
    return thread






def group_by_hash(analyses):
    groups = {}
    for page, analysis, error in analyses:
        if analysis:
            key = analysis['text']['diff_hash']
        else:
            key = '__ERROR__'
        if key not in groups:
            groups[key] = {'items': [], 'priority': 0}

        groups[key]['items'].append((page, analysis, error))
        groups[key]['priority'] = max(groups[key]['priority'],
                                      analysis and analysis['priority'] or 0)

    for key, group in groups.items():
        group['items'].sort(key=lambda x: x[1] and x[1]['priority'] or 0,
                            reverse=True)

    return groups


def group_by_tags(analyses: Iterable[PageAnalysisResult], tag_types=None) -> dict[str, list[PageAnalysisResult]]:
    tag_types = tag_types or ['domain:']
    groups = defaultdict(list)
    for result in analyses:
        group_parts = []
        for prefix in tag_types:
            for tag in result.page['tags']:
                if tag['name'] == prefix:
                    group_parts.append(prefix)
                    break
                if tag['name'].startswith(prefix):
                    group_parts.append(tag['name'][len(prefix):])
                    break
        group_name = '--'.join(group_parts)
        groups[group_name].append(result)

    return groups


def log_error(output, verbose, item):
    page, _, error = item
    if error and not isinstance(error, analyze.AnalyzableError):
        output.write(f'ERROR {page["uuid"]}: {error}')
        if verbose:
            if hasattr(error, 'traceback'):
                output.write('\n'.join(error.traceback))
            else:
                traceback.print_tb(error.__traceback__, file=output)


def pretty_print_analysis(page, analysis, output=None):
    message = [f'{page["url"]} ({page["uuid"]}):']
    a = page['versions'][len(page['versions']) - 1]
    b = page['versions'][0]
    message.append(f'  a: {a["uuid"]}\n  b: {b["uuid"]}')
    for key, value in analysis.items():
        message.append(f'  {key}: {value}')

    message = '\n'.join(message)
    if output:
        output.write(message)
    else:
        print(message, file=sys.stderr)


def pretty_print_binary_analysis(result: PageAnalysisResult, output=None):
    def analysis_output(analysis: dict) -> list[str]:
        return [
            f'    {key}: {value}'
            for key, value in analysis.items()
        ]

    message = [f'{result.page["url"]} ({result.page["uuid"]}):']
    if result.error:
        message.append(f'  {result.error}')
    else:
        message.append(f'  {len(result.changes)} found changes')
        message.append(f'  Overall period:')
        message.extend(analysis_output(result.overall))
        for change in result.changes:
            a = change['versions'][-1]
            b = change['versions'][0]
            message.append(f'  a: {a["uuid"]} ({a["capture_time"]})')
            message.append(f'  b: {b["uuid"]} ({b["capture_time"]})')
            message.extend(analysis_output(change['analysis']))

    message = '\n'.join(message)
    if output:
        output.write(message)
    else:
        print(message, file=sys.stderr)


def filter_priority(results: list, threshold: float = 0) -> Iterable:
    return filter(
        lambda item: item[1] is None or item[1]['priority'] >= threshold,
        results
    )


def write_sheets(output_path: Path, results: Iterable[PageAnalysisResult]):
    sheet_groups = group_by_tags(results, ['category:', 'news', '2l-domain:'])
    for sheet_name, sheet_results in sheet_groups.items():
        sorted_results = sorted(
            sheet_results,
            key=lambda r: (r.overall['priority'] if r.overall else 0),
            reverse=True
        )
        write_csv(output_path, sheet_name, sorted_results)


from analyst_sheets import sheets
import csv
import re


def write_csv(parent_directory: Path, name: str, results: Iterable[PageAnalysisResult]):
    """
    Write a CSV to disk with rows representing changes found tuples.
    """
    filename = re.sub(r'[:/]', '_', name) + '.csv'
    filepath = parent_directory / filename

    timestamp = datetime.utcnow().isoformat() + 'Z'

    with filepath.open('w') as file:
        writer = csv.writer(file)
        writer.writerow(sheets.HEADERS)

        index = 0
        for result in results:
            maintainers = ', '.join(m['name'] for m in result.page['maintainers'])

            index += 1
            page_row = [
                index,
                'OVERALL',
                timestamp,
                maintainers,
                name,
                sheets.clean_string(result.page['title']),
                result.page['url'],
                '---',
                sheets.create_view_url(result.page, result.timeframe[-1], result.timeframe[0]),
                sheets.create_ia_changes_url(result.page, result.timeframe[-1], result.timeframe[0]),
                result.timeframe[0]['capture_time'].isoformat(),
                result.timeframe[-1]['capture_time'].isoformat(),
            ]

            if result.overall:
                analysis = result.overall
                page_row.extend([
                    analysis['source']['diff_length'],
                    sheets.format_hash(analysis['source']['diff_hash']),
                    analysis['text']['diff_length'],
                    sheets.format_hash(analysis['text']['diff_hash']),
                    None,
                    format(analysis['priority'], '.3f'),
                    '',

                    analysis['root_page'],
                    analysis['status_changed'],
                    analysis['status_b'],
                    result.timeframe[0]['status'],
                    analysis['text']['readable'],
                    ', '.join((f'{term}: {count}' for term, count in analysis['text']['key_terms'].items())),
                    format(analysis['text']['percent_changed'], '.3f'),
                    analysis['text']['diff_max_length'],
                    sheets.format_hash(analysis['links']['diff_hash']),
                    analysis['links']['diff_length'],
                    format(analysis['links']['diff_ratio'], '.3f'),
                    analysis['links']['removed_self_link'],
                    analysis['redirect']['is_client_redirect'] or '',
                    analysis['redirect']['changed'] or '',
                    sheets.format_redirects(analysis['redirect']['a_server'], analysis['redirect']['a_client']),
                    sheets.format_redirects(analysis['redirect']['b_server'], analysis['redirect']['b_client']),
                ])
            else:
                page_row.extend([
                    None,
                    None,
                    None,
                    None,
                    None,
                    '?',
                    str(result.error)
                ])
            writer.writerow(page_row)

            if result.changes:
                for change in result.changes:
                    index += 1
                    analysis = change['analysis']
                    timeframe = change['versions']
                    row = [
                        index,
                        timeframe[0]['uuid'],
                        timestamp,
                        maintainers,
                        name,
                        sheets.clean_string(result.page['title']),
                        result.page['url'],
                        '---',
                        sheets.create_view_url(result.page, timeframe[-1], timeframe[0]),
                        sheets.create_ia_changes_url(result.page, timeframe[-1], timeframe[0]),
                        timeframe[0]['capture_time'].isoformat(),
                        timeframe[-1]['capture_time'].isoformat(),
                        analysis['source']['diff_length'],
                        sheets.format_hash(analysis['source']['diff_hash']),
                        analysis['text']['diff_length'],
                        sheets.format_hash(analysis['text']['diff_hash']),
                        None,
                        format(analysis['priority'], '.3f'),
                        '',

                        analysis['root_page'],
                        analysis['status_changed'],
                        analysis['status_b'],
                        result.timeframe[0]['status'],
                        analysis['text']['readable'],
                        ', '.join((f'{term}: {count}' for term, count in analysis['text']['key_terms'].items())),
                        format(analysis['text']['percent_changed'], '.3f'),
                        analysis['text']['diff_max_length'],
                        sheets.format_hash(analysis['links']['diff_hash']),
                        analysis['links']['diff_length'],
                        format(analysis['links']['diff_ratio'], '.3f'),
                        analysis['links']['removed_self_link'],
                        analysis['redirect']['is_client_redirect'] or '',
                        analysis['redirect']['changed'] or '',
                        sheets.format_redirects(analysis['redirect']['a_server'], analysis['redirect']['a_client']),
                        sheets.format_redirects(analysis['redirect']['b_server'], analysis['redirect']['b_client']),
                    ]
                    writer.writerow(row)

            writer.writerow(['---' for _ in sheets.HEADERS])


def format_row(page, analysis, error, index, name, timestamp):
    version_start = page['versions'][len(page['versions']) - 1]
    version_end = page['versions'][0]

    row = [
        index + 1,
        version_end['uuid'],
        timestamp,
        ', '.join(m['name'] for m in page['maintainers']),
        name,
        clean_string(page['title']),
        page['url'],
        '',
        create_view_url(page, version_start, version_end),
        # Empty column for "latest to base"; it's only present to preserve
        # column order for pasting into the significant changes sheet.
        create_ia_changes_url(page, version_start, version_end),
        version_end['capture_time'].isoformat(),
        # Empty column for earliest capture time. It's unused and only present
        # to preserve column order for pasting into other spreadsheets.
        '',
    ]

    if analysis:
        row.extend([
            analysis['source']['diff_length'],
            format_hash(analysis['source']['diff_hash']),
            analysis['text']['diff_length'],
            format_hash(analysis['text']['diff_hash']),
            len(page['versions']),
            format(analysis['priority'], '.3f'),
            '',

            analysis['root_page'],
            analysis['status_changed'],
            analysis['status_b'],
            version_end['status'],
            analysis['text']['readable'],
            ', '.join((f'{term}: {count}' for term, count in analysis['text']['key_terms'].items())),
            format(analysis['text']['percent_changed'], '.3f'),
            analysis['text']['diff_max_length'],
            format_hash(analysis['links']['diff_hash']),
            analysis['links']['diff_length'],
            format(analysis['links']['diff_ratio'], '.3f'),
            analysis['links']['removed_self_link'],
            analysis['redirect']['is_client_redirect'] or '',
            analysis['redirect']['changed'] or '',
            format_redirects(analysis['redirect']['a_server'], analysis['redirect']['a_client']),
            format_redirects(analysis['redirect']['b_server'], analysis['redirect']['b_client']),
        ])
    else:
        row.extend([
            None,
            None,
            None,
            None,
            len(page['versions']),
            '?',
            str(error)
        ])

    return row


def main(pattern=None, tags=None, after=None, before=None, output_path=None, threshold=0, verbose=False, use_readability=True):
    from pprint import pp
    with QuitSignal((signal.SIGINT,)) as cancel:
        # Make sure we can actually output the results before getting started.
        if output_path:
            output_path.mkdir(exist_ok=True)


        pages = list_all_pages(pattern, after, before, tags, cancel=cancel, total=True)
        page_count = next(pages)
        page_load_progress = tqdm(pages, desc='loading', unit=' pages', total=page_count)
        raw_results = analyze_pages(page_load_progress, after, before, use_readability, threshold, cancel)
        results = tqdm(raw_results, desc='analyzing', unit=' pages', total=page_count)
        # results = tap(results, lambda result: pretty_print_binary_analysis(result, tqdm))
        results = list(results)

        if output_path:
            def serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, Exception):
                    return f'{type(obj).__name__}: {obj}'
                # return f'!!! type:{type(obj)} !!!'
                raise TypeError(f'Cannot JSON serialize {type(obj)}')

            count = len(results)
            sorted_results = sorted(results, key=lambda r: r.page['url_key'])
            with gzip.open(output_path / '_results.json.gz', 'wt', encoding='utf-8') as f:
                f.write('[\n')
                for index, result in enumerate(sorted_results):
                    serializable_page = result.page.copy()
                    del serializable_page['_list_meta']
                    del serializable_page['_list_links']
                    if 'versions' in serializable_page:
                        del serializable_page['versions']

                    for change in result.changes:
                        for version in change['versions']:
                            # FIXME: should not have to clean these up. Should not be
                            # attached to version objects in analyze module.
                            version.pop('response', None)
                            version.pop('normalized', None)
                            version.pop('_list_meta', None)
                            version.pop('_list_links', None)

                    json.dump({
                        'page': serializable_page,
                        'overall': result.overall,
                        'changes': result.changes,
                        'error': result.error
                    }, f, default=serializer)
                    if index + 1 < count:
                        f.write(',\n')

                f.write('\n]')

        filtered = [
            result
            for result in results
            if result.error is None and result.overall['priority'] >= threshold
        ]
        if output_path:
            write_sheets(output_path, filtered)
        else:
            for result in filtered:
                pretty_print_binary_analysis(result, output=tqdm)

        # Clear the last line from TQDM, which seems to leave behind the second
        # progress bar. :\
        print('', file=sys.stderr)


def timeframe_date(date_string):
    """
    Parse a CLI date string into a UTC datetime object. The input can be an
    ISO 8601 timestamp (e.g. '2019-11-02T00:00:00Z') or a number of hours
    before the current time (e.g. '48' for two days ago).
    """
    try:
        hours = float(date_string)
        return datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    except ValueError:
        # Parse as timestamp, ensure it's in UTC.
        return dateutil.parser.parse(date_string).astimezone(tzutc())


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Count term changes in monitored pages.')
    parser.add_argument('--output', type=Path, help='Output CSV files in this directory')
    parser.add_argument('--pattern', help='Only analyze pages with URLs matching this pattern.')
    parser.add_argument('--tag', action='append', help='Only analyze pages with this tag (repeat for multiple tags).')
    parser.add_argument('--after', type=timeframe_date, help='Only include versions after this date. May also be a number of hours before the current time.')
    parser.add_argument('--before', type=timeframe_date, help='Only include versions before this date. May also be a number of hours before the current time.')
    parser.add_argument('--threshold', type=float, default=0.5, help='Minimum priority value to include in output.')
    parser.add_argument('--verbose', action='store_true', help='Show detailed error messages')
    parser.add_argument('--skip-readability', dest='use_readability', action='store_false', help='Do not use readability to parse pages.')
    # Need the ability to actually start/stop the readability server if we want this option
    # parser.add_argument('--readability', action='store_true', help='Only analyze pages with URLs matching this pattern.')
    options = parser.parse_args()

    # Validate before vs. after
    if options.before and options.after and options.before <= options.after:
        print('--before must indicate a date after --after', file=sys.stderr)
        sys.exit(1)

    # Some analysis calculations need to understand the timeframe in question,
    # so make sure we always have a start and end date to keep things simple.
    if options.after is None:
        options.after = datetime(2000, 1, 1, tzinfo=timezone.utc)
    if options.before is None:
        options.before = datetime.now(tz=timezone.utc)

    main(pattern=options.pattern,
         tags=(options.tag or None),
         before=options.before,
         after=options.after,
         output_path=options.output,
         threshold=options.threshold,
         verbose=options.verbose,
         use_readability=options.use_readability)
