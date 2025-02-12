from analyst_sheets import analyze
from analyst_sheets.sheets import write_csv
from analyst_sheets.tools import generate_on_thread, map_parallel, QuitSignal, ActivityMonitor, tap
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import dateutil.parser
from dateutil.tz import tzutc
from itertools import islice
import json
from pathlib import Path
from retry import retry
import signal
import sys
from tqdm import tqdm
import threading
import traceback
from web_monitoring import db


client_storage = threading.local()
def get_thread_client():
    if not hasattr(client_storage, 'client'):
        client_storage.client = db.Client.from_env()
    return client_storage.client


def list_all_pages(url_pattern, after, before, tags=None, cancel=None, client=None, total=False):
    client = client or get_thread_client()

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
    client = client or get_thread_client()

    if cancel and cancel.is_set():
        return

    versions = client.get_versions(page_id=page_id,
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


# def get_earliest_version(page_id, cancel=None, client=None):
#     client = client or get_thread_client()

#     if cancel and cancel.is_set():
#         return

#     return next(client.get_versions(page_id=page_id,
#                                     sort=['capture_time:asc'],
#                                     chunk_size=1))


def maybe_bad_capture(version) -> bool:
    """
    Identify captures that are likely to have been blocked responses (e.g.
    a rate limit or firewall rule blocked the crawler) or intermittent errors.
    These don't represent what a regular user should have seen at the time, so
    we should avoid using them as candidates for comparison.
    """
    headers = version['headers'] or {}
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
    elif server == 'cloudfront' and cache_error:
        return True
    # TODO: see if we have any Azure CDN examples?
    # TODO: More general heuristics?
    # else:
    #     content_type = version['media_type'] or headers.get('content-type', '')
    #     x_cache = headers.get('x-cache', '').lower()
    #     cache_miss = x_cache and not x_cache.startswith('hit')
    #     return content_type.startswith('text/html') and is_short_or_unknown and cache_miss
    return False


def add_versions_to_page(page, after, before):
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

    page['versions'] = versions
    return page


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


def group_by_tags(analyses, tag_types=None):
    tag_types = tag_types or ['domain:']
    groups = defaultdict(list)
    for page, analysis, error in analyses:
        group_parts = []
        for prefix in tag_types:
            for tag in page['tags']:
                if tag['name'] == prefix:
                    group_parts.append(prefix)
                    break
                if tag['name'].startswith(prefix):
                    group_parts.append(tag['name'][len(prefix):])
                    break
        group_name = '--'.join(group_parts)
        groups[group_name].append((page, analysis, error))

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


def main(pattern=None, tags=None, after=None, before=None, output_path=None, threshold=0, verbose=False, use_readability=True):
    with QuitSignal((signal.SIGINT,)) as cancel:
        # Make sure we can actually output the results before getting started.
        if output_path:
            output_path.mkdir(exist_ok=True)

        pages = generate_on_thread(list_all_pages, pattern, after, before,
                                   tags, cancel=cancel, total=True).output

        page_count = next(pages)
        pages_and_versions = map_parallel(add_versions_to_page, pages, after,
                                          before, cancel=cancel,
                                          parallel=4).output

        # Separate progress bar just for data loading from Scanner's DB
        pages_and_versions = tqdm(pages_and_versions, desc='  loading',
                                  unit=' pages', total=page_count)

        def iterate_all(source):
            yield from source
        pages_and_versions_queue = generate_on_thread(iterate_all, pages_and_versions, cancel=cancel).output
        results = analyze.analyze_pages(pages_and_versions_queue, after, before, use_readability=use_readability, cancel=cancel)
        # results = analyze.analyze_pages(pages_and_versions, after, before, cancel=cancel)
        progress = tqdm(results, desc='analyzing', unit=' pages', total=page_count)
        # Log any unexpected errors along the way.
        results = tap(progress, lambda result: log_error(tqdm, verbose, result))
        # Don't output pages where there was no overall change.
        results = (item for item in results if not isinstance(item[2], analyze.NoChangeError))

        # DEBUG READABILITY FALLBACK CODE
        results = list(results)
        no_content_readable = []
        no_content_unreadable = []
        for page, analysis, error in results:
            if not error and not analysis['text']['found_content_area']:
                if analysis['redirect']['is_client_redirect']:
                    continue
                a_id = page['versions'][len(page['versions']) - 1]['uuid']
                b_id = page['versions'][0]['uuid']
                if analysis['text']['readable']:
                    no_content_readable.append((page['url'], page['uuid'], a_id, b_id))
                else:
                    no_content_unreadable.append((page['url'], page['uuid'], a_id, b_id))
        def sortable_entry(entry):
            if entry[0].startswith('http:'):
                return (f'https{entry[0][4:]}', *entry[1:])
            return entry
        no_content_readable.sort(key=sortable_entry)
        no_content_unreadable.sort(key=sortable_entry)
        readability_debug = []
        if len(no_content_readable):
            readability_debug.append('No content found, but readability worked:')
            for url, page_id, a_id, b_id in no_content_readable:
                readability_debug.append(f'  {url}: https://api.monitoring.envirodatagov.org/api/v0/pages/{page_id}/changes/{a_id}..{b_id}')
        if len(no_content_unreadable):
            readability_debug.append('No content found, AND readability failed:')
            for url, page_id, a_id, b_id in no_content_unreadable:
                readability_debug.append(f'  {url}: https://api.monitoring.envirodatagov.org/api/v0/pages/{page_id}/changes/{a_id}..{b_id}')
        if output_path:
            with (output_path / '_readability_debug.txt').open('w') as f:
                f.write('\n'.join(readability_debug) + '\n')
        else:
            print('\n'.join(readability_debug), file=sys.stderr)
        # END DEBUG READABLILITY FALLBACK

        # TODO: should probably be moved to another module?
        if output_path:
            def serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                # return f'!!! type:{type(obj)} !!!'
                raise TypeError(f'Cannot JSON serialize {type(obj)}')

            count = len(results)
            sorted_results = sorted(results, key=lambda r: r[0]['url_key'])
            with (output_path / 'results.json').open('w') as f:
                f.write('[\n')
                for index, (page, analysis, error) in enumerate(sorted_results):
                    serializable_page = page.copy()
                    del serializable_page['_list_meta']
                    del serializable_page['_list_links']

                    # FIXME: should not have to clean these up. Should not be
                    # attached to version objects in analyze module.
                    first_version = page['versions'][0].copy()
                    del first_version['response']
                    del first_version['normalized']
                    del first_version['_list_meta']
                    del first_version['_list_links']
                    if len(page['versions']) > 0:
                        serializable_page['versions'] = [
                            first_version,
                            *[{} for v in page['versions'][1:-1]]
                        ]
                    if len(page['versions']) > 1:
                        last_version = page['versions'][-1].copy()
                        del last_version['response']
                        del last_version['normalized']
                        del last_version['_list_meta']
                        del last_version['_list_links']
                        serializable_page['versions'].append(last_version)

                    json.dump({
                        'page': serializable_page,
                        'analysis': analysis,
                        'error': error
                    }, f, default=serializer)
                    if index + 1 < count:
                        f.write(',\n')

                f.write('\n]')

        # Filter out results under the threshold
        results = filter(lambda item: item[1] is None or item[1]['priority'] >= threshold,
                         results)

        # If we aren't writing to disk, just print the high-priority results.
        if not output_path:
            for page, analysis, error in results:
                if analysis:  # and analysis['priority'] >= 0: # 0.5:
                    pretty_print_analysis(page, analysis, tqdm)
            return

        # Otherwise, prepare spreadsheets and write them to disk!
        sheet_groups = group_by_tags(results, ['2l-domain:', '2025-seed-category:', 'news'])
        for sheet_name, results in sheet_groups.items():
            # Group results into similar changes and sort those groups.
            grouped_results = group_by_hash(results)
            sorted_groups = sorted(grouped_results.values(),
                                   key=lambda group: group['priority'],
                                   reverse=True)
            # Flatten the groups back into individual results.
            sorted_rows = (item
                           for result in sorted_groups
                           for item in result['items'])
            write_csv(output_path, sheet_name, sorted_rows)

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
    parser.add_argument('--tag', action='append', help='Only anlyze pages with this tag (repeat for multiple tags).')
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
