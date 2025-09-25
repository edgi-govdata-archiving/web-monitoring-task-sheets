from analyst_sheets.analyze import get_version_status, load_url
import csv
from datetime import datetime, timedelta, timezone
from generate_task_sheets import (
    list_all_pages,
    add_versions_to_page,
    list_page_versions,
)
import re
from sys import stderr, stdout
from tqdm import tqdm


def format_datetime(value: datetime | None) -> str:
    if value is None:
        return ''

    value = value.replace(microsecond=0)
    return re.sub(r'\+00:00$', 'Z', value.isoformat())


def format_status(value: str | int | None) -> str:
    if value is None or value == 600 or value == '':
        return '(offline)'

    return str(value)


before = datetime.now(tz=timezone.utc)
after = before - timedelta(days=14)

pages = list_all_pages('*', after=None, before=None, tags=['PEDP'], total=True)
count = next(pages)
progress = tqdm(pages, total=count)

csv_writer = csv.writer(stdout)
csv_writer.writerow([
    'URL',
    'Status',
    'Effective Status',
    'Capture Time',
    'Latest Valid Capture Time',
    'Scanner URL'
])

for index, page in enumerate(progress):
    # if index > 0 and index % 25 == 0:
    #     progress.write('Chilling for a moment...', file=stderr)
    #     sleep(10)

    page = add_versions_to_page(page, after=after, before=before)
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

    csv_writer.writerow([
        page['url'],
        format_status(latest['status']),
        format_status(effective_status),
        format_datetime(latest['capture_time']),
        format_datetime(latest_valid and latest_valid['capture_time']),
        f'https://monitoring.envirodatagov.org/page/{page["uuid"]}'
    ])
