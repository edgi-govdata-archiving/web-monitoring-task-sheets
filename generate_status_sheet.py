from analyst_sheets.analyze import get_version_status, load_url
from analyst_sheets.sheets import format_datetime, format_status
import csv
from datetime import datetime, timedelta, timezone
from generate_task_sheets import (
    list_all_pages,
    add_versions_to_page,
    list_page_versions,
)
from sys import stderr, stdout
from tqdm import tqdm


def main(url: str = '*', tags: list[str] = []) -> None:
    before = datetime.now(tz=timezone.utc)
    after = before - timedelta(days=14)

    pages = list_all_pages(url, after=None, before=None, tags=tags, total=True)
    count = next(pages)
    progress = tqdm(pages, total=count)

    csv_writer = csv.writer(stdout)
    csv_writer.writerow([
        'Agencies',
        'Offices',
        'URL',
        'Status',
        'Effective Status',
        'Capture Time',
        'Latest Valid Capture Time',
        'Scanner URL'
    ])

    for page in progress:
        page = add_versions_to_page(
            page,
            after=after,
            before=before,
            candidates=list_page_versions(page['uuid'], None, before, chunk_size=20)
        )
        latest = next(list_page_versions(page['uuid'], None, before, chunk_size=1))
        latest_valid = page['versions'][0] if len(page['versions']) else latest
        if not latest_valid:
            progress.write(
                f'Skipping {page["uuid"]}: No good versions',
                file=stderr
            )
            continue

        latest_valid['response'] = load_url(
            latest_valid['body_url'],
            timeout=20,
            headers={'accept': '*/*'}
        )
        effective_status = get_version_status(latest_valid)

        page_agencies = [
            tag['name'].split(':', 1)[1]
            for tag in page['tags']
            if tag['name'].startswith('agency:')
        ]
        page_offices = [
            tag['name'].split(':', 1)[1]
            for tag in page['tags']
            if tag['name'].startswith('office:')
        ]

        csv_writer.writerow([
            ', '.join(sorted(page_agencies)),
            ', '.join(sorted(page_offices)),
            page['url'],
            format_status(latest['status']),
            format_status(effective_status),
            format_datetime(latest['capture_time']),
            format_datetime(latest_valid and latest_valid['capture_time']),
            f'https://monitoring.envirodatagov.org/page/{page["uuid"]}'
        ])


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Check status codes of monitored pages.')
    parser.add_argument('--url', default='*', help='Only check pages with URLs matching this pattern.')
    parser.add_argument('--tag', action='append', help='Only check pages with this tag (repeat for multiple tags).')
    options = parser.parse_args()

    tags = [
        tag
        for cli_tag in options.tag
        for tag in cli_tag.split(',')
    ]

    main(url=options.url, tags=tags)
