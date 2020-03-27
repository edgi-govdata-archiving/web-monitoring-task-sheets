from analyst_sheets import analyze
from analyst_sheets.sheets import write_csv
from analyst_sheets.tools import generate_on_thread, map_parallel, QuitSignal, ActivityMonitor, tap
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import dateutil.parser
from dateutil.tz import tzutc
from pathlib import Path
from retry import retry
import signal
import sys
from tqdm import tqdm
import traceback
from web_monitoring import db


def list_all_pages(url_pattern, after, before, cancel=None, client=None, total=False):
    if client is None:
        client = db.Client.from_env()

    chunk = 1
    while chunk > 0:
        if cancel and cancel.is_set():
            return

        pages = request_page_chunk(client, sort=['created_at:asc'],
                                   chunk_size=1000, chunk=chunk,
                                   url=url_pattern, active=True,
                                   start_date=after, end_date=before,
                                   include_earliest=True,
                                   include_total=(total and chunk == 1))
        if total and chunk == 1:
            yield pages["meta"]["total_results"]
        yield from pages['data']
        chunk = pages['links']['next'] and (chunk + 1) or -1


def list_page_versions(page_id, after, before, cancel=None, client=None):
    if client is None:
        client = db.Client.from_env()

    common = dict(page_id=page_id,
                  include_change_from_previous=True,
                  include_change_from_earliest=True,
                  sort=['capture_time:desc'])

    chunk = 1
    while chunk > 0:
        if cancel and cancel.is_set():
            return

        result = request_version_chunk(client, chunk=chunk, chunk_size=1000,
                                       start_date=after, end_date=before,
                                       **common)
        yield from result['data']
        chunk = result['links']['next'] and (chunk + 1) or -1

    if cancel and cancel.is_set():
        return

    # Get version leading into timeframe.
    result = request_version_chunk(client, chunk_size=1,
                                   start_date=None, end_date=after,
                                   **common)
    yield from result['data']


@retry(tries=3, delay=1)
def request_page_chunk(client, **kwargs):
    with ActivityMonitor(f'List pages: {kwargs}'):
        return client.list_pages(**kwargs)


@retry(tries=3, delay=1)
def request_version_chunk(client, **kwargs):
    with ActivityMonitor(f'List versions: {kwargs}'):
        return client.list_versions(**kwargs)


def add_versions_to_page(page, after, before):
    """
    Find all the relevant versions of a page in the given timeframe and attach
    them to the page object as 'versions'.
    """
    versions = list(list_page_versions(page['uuid'], after, before))

    # If the latest version is an error but the page is not in an error state,
    # then the error was spurious and should not be part of the analysis.
    if page['status'] < 400:
        error_versions = []
        while versions and versions[0]['status'] >= 400:
            error_versions.append(versions.pop(0))

        if error_versions:
            page['error_versions'] = error_versions

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
            traceback.print_tb(error.__traceback__, file=output)


def pretty_print_analysis(page, analysis, output=None):
    message = [f'{page["url"]} ({page["uuid"]}):']
    for key, value in analysis.items():
        message.append(f'  {key}: {value}')

    message = '\n'.join(message)
    if output:
        output.write(message)
    else:
        print(message, file=sys.stderr)


def main(pattern=None, after=None, before=None, output_path=None, verbose=False):
    with QuitSignal((signal.SIGINT,)) as cancel:
        # Make sure we can actually output the results before getting started.
        if output_path:
            output_path.mkdir(exist_ok=True)

        pages = generate_on_thread(list_all_pages, pattern, after, before,
                                   cancel=cancel, total=True).output

        page_count = next(pages)
        pages_and_versions = map_parallel(add_versions_to_page, pages, after,
                                          before, cancel=cancel,
                                          parallel=5).output

        # Separate progress bar just for data loading from Scanner's DB
        # pages_and_versions = tqdm(pages_and_versions, desc='  loading',
        #                           unit=' pages', total=page_count)

        results = analyze.analyze_pages(pages_and_versions, after, before, cancel=cancel)
        progress = tqdm(results, desc='analyzing', unit=' pages', total=page_count)
        # Log any unexpected errors along the way.
        results = tap(progress, lambda result: log_error(tqdm, verbose, result))
        # Don't output pages where there was no overall change.
        results = (item for item in results if not isinstance(item[2], analyze.NoChangeError))

        # If we aren't writing to disk, just print the high-priority results.
        if not output_path:
            for page, analysis, error in results:
                if analysis and analysis['priority'] >= 0.5:
                    pretty_print_analysis(page, analysis, tqdm)
            return

        # Otherwise, prepare spreadsheets and write them to disk!
        sheet_groups = group_by_tags(results, ['2l-domain:', 'tag2:', 'news'])
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
    parser.add_argument('--after', type=timeframe_date, help='Only include versions after this date. May also be a number of hours before the current time.')
    parser.add_argument('--before', type=timeframe_date, help='Only include versions before this date. May also be a number of hours before the current time.')
    parser.add_argument('--verbose', action='store_true', help='Show detailed error messages')
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
         before=options.before,
         after=options.after,
         output_path=options.output,
         verbose=options.verbose)
