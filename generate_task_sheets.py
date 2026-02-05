from analyst_sheets import analyze
from analyst_sheets.sheets import write_csv
from analyst_sheets.tools import (
    get_thread_db_client,
    tap
)
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import dateutil.parser
from dateutil.tz import tzutc
import functools
import gzip
from itertools import islice
import json
import multiprocessing
import multiprocessing.pool
from pathlib import Path
import signal
import sys
from typing import Iterable
from tqdm import tqdm
import threading
import traceback
from web_monitoring.utils import QuitSignal, Signal


@dataclass
class PageAnalysisResult:
    page: dict
    timeframe: list[dict]
    overall: dict | None = None
    changes: list[dict] = field(default_factory=list)
    error: Exception | None = None


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


def add_versions_to_page(page: dict, after: datetime, before: datetime, candidates: Iterable[dict]) -> dict:
    """
    Find all the relevant versions of a page in the given timeframe and attach
    them to the page object as 'versions'.
    """
    def in_time_range(version) -> bool:
        return version['capture_time'] >= after and version['capture_time'] < before

    versions = []
    questionable_versions = []
    for version in candidates:
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
                for candidate in islice(candidates, 100):
                    if (baseline['capture_time'] - candidate['capture_time']).days > 30:
                        break
                    elif not maybe_bad_capture(candidate):
                        baseline = candidate
                        break

            versions.append(baseline)
            break

    page['versions'] = trim_version_range(versions)
    return page


def analyze_page(after: datetime, before: datetime, use_readability: bool, threshold: float, deep: bool, page: dict) -> PageAnalysisResult:
    """
    Search for and analyze the relevant changes in a page.
    """
    candidate_versions = list_page_versions(page['uuid'], None, before, chunk_size=20)
    full_period = add_versions_to_page(page.copy(), after, before, candidate_versions)

    result = PageAnalysisResult(
        page=page,
        timeframe=[],
    )

    if len(full_period['versions']) >= 2:
        result.timeframe = [full_period['versions'][0], full_period['versions'][-1]]
    else:
        result.error = analyze.NoChangeError('Page has no changed versions')
        return result

    _, overall, error = analyze.work_page(after, before, use_readability, full_period)
    if error:
        result.error = error
    else:
        result.overall = overall
        if deep and overall['priority'] >= threshold:
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
        # print(
        #     f"{indent}Period {page['uuid']} {period[-1]['capture_time']} to "
        #     f"{period[0]['capture_time']}"
        # )
        if period[0]['body_hash'] == period[-1]['body_hash']:
            # print(f"{indent}No change, skipping")
            continue

        period_after = period[-1]['capture_time'] + timedelta(minutes=1)
        period_before = period[0]['capture_time'] + timedelta(minutes=1)
        period_page = add_versions_to_page(page.copy(), period_after, period_before, period)

        if len(period_page['versions']) < 2:
            # print(f"{indent}No change after trim, skipping")
            continue

        _, period_result, error = analyze.work_page(period_after, period_before, use_readability, period_page)
        if isinstance(error, analyze.NoChangeError):
            # print(f"{indent}(No change)")
            ...
        elif error:
            # print(f'{indent}UHOH: {error}')
            raise error
        elif period_result['priority'] >= threshold:
            # print(f"{indent}priority={period_result['priority']}")
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


def setup_worker():
    # Ignore sigint because the main process is handling it.
    # Total abuse of the context manager protocol :\
    handler = Signal((signal.SIGINT,), signal.SIG_IGN)
    handler.__enter__()


def analyze_pages(pages: dict, after: datetime, before: datetime, use_readability: bool = True, threshold: float = 0.25, deep: bool = False, cancel: threading.Event = None):
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

        work = functools.partial(analyze_page, after, before, use_readability, threshold, deep)
        yield from pool.imap_unordered(work, pages)
        pool.close()


def close_on_event(pool: multiprocessing.pool.Pool, event):
    def wait_and_close():
        event.wait()
        pool.close()

    thread = threading.Thread(target=wait_and_close, daemon=True)
    thread.start()
    return thread


def group_by_hash(analyses: Iterable[PageAnalysisResult]):
    groups = {}
    for result in analyses:
        if result.overall:
            key = result.overall['text']['diff_hash']
        else:
            key = '__ERROR__'
        if key not in groups:
            groups[key] = {'items': [], 'priority': 0}

        groups[key]['items'].append(result)
        groups[key]['priority'] = max(groups[key]['priority'],
                                      result.overall and result.overall['priority'] or 0)

    for key, group in groups.items():
        group['items'].sort(key=lambda x: x.overall and x.overall['priority'] or 0,
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


def log_error(output, verbose, item: PageAnalysisResult) -> None:
    if item.error and not isinstance(item.error, analyze.AnalyzableError):
        output.write(f'ERROR {item.page["uuid"]}: {item.error}')
        if verbose:
            if hasattr(item.error, 'traceback'):
                output.write('\n'.join(item.error.traceback))
            else:
                traceback.print_tb(item.error.__traceback__, file=output)


def pretty_print_analysis(result: PageAnalysisResult, output=None):
    message = [f'{result.page["url"]} ({result.page["uuid"]}):']
    if result.error:
        message.append(f'  {result.error}')
    else:
        changes = [{'versions': result.timeframe, 'analysis': result.overall}] + result.changes
        for index, change in enumerate(changes):
            if index == 0:
                message.append('  Overall period:')
            elif index == 1:
                message.append(f'  {len(result.changes)} narrower changes:')

            a = change['versions'][-1]
            b = change['versions'][0]
            message.append(f'  a: {a["uuid"]} ({a["capture_time"]})')
            message.append(f'  b: {b["uuid"]} ({b["capture_time"]})')
            for key, value in change['analysis'].items():
                message.append(f'    {key}: {value}')

    message = '\n'.join(message)
    if output:
        output.write(message)
    else:
        print(message, file=sys.stderr)


def write_sheets(output_path: Path, results: Iterable[PageAnalysisResult], deep: bool = False):
    sheet_groups = group_by_tags(results, ['category:', 'news', '2l-domain:'])
    for sheet_name, sheet_results in sheet_groups.items():
        sorted_results = []
        # Group results by text diff hash only when not doing deep analysis.
        # it's tough to deal with more than one level of grouping in sheets.
        # TODO: maybe reconsider this?
        if deep:
            sorted_results = sorted(
                sheet_results,
                key=lambda r: (r.overall['priority'] if r.overall else 0),
                reverse=True
            )
        else:
            grouped_results = group_by_hash(sheet_results)
            sorted_groups = sorted(
                grouped_results.values(),
                key=lambda group: group['priority'],
                reverse=True
            )
            # Flatten the groups back into individual results.
            sorted_results = (
                item
                for result in sorted_groups
                for item in result['items']
            )

        write_csv(output_path, sheet_name, sorted_results, deep)


def main(pattern=None, tags=None, after=None, before=None, output_path=None, threshold=0, deep=False, verbose=False, use_readability=True):
    with QuitSignal((signal.SIGINT,)) as cancel:
        # Make sure we can actually output the results before getting started.
        if output_path:
            output_path.mkdir(exist_ok=True)

        pages = list_all_pages(pattern, after, before, tags, cancel=cancel, total=True)
        page_count = next(pages)
        results = analyze_pages(pages, after, before, use_readability, threshold, deep, cancel)
        results = tqdm(results, desc='analyzing', unit=' pages', total=page_count)
        results = tap(results, lambda result: log_error(tqdm, verbose, result))
        # results = tap(results, lambda result: pretty_print_analysis(result, tqdm))
        results = [r for r in results if not isinstance(r.error, analyze.NoChangeError)]

        if output_path:
            def serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, Exception):
                    return f'{type(obj).__name__}: {obj}'
                raise TypeError(f'Cannot JSON serialize {type(obj)}')

            print('Writing raw data...')
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

        print('Writing spreadsheets...')
        filtered = [
            result
            for result in results
            if result.overall is None or result.overall['priority'] >= threshold
        ]
        if output_path:
            write_sheets(output_path, filtered, deep)
        else:
            for result in filtered:
                pretty_print_analysis(result, output=tqdm)

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
    parser.add_argument('--deep', action='store_true', help='Do a deep analysis and find every notable change on each page, rather than just analyzing the whole time period.')
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
         deep=options.deep,
         verbose=options.verbose,
         use_readability=options.use_readability)
