from datetime import datetime, timedelta, timezone
from generate_task_sheets import (
    list_all_pages,
    add_versions_to_page,
    list_page_versions,
)
from analyst_sheets.analyze import get_version_status, load_url
from tqdm import tqdm
from time import sleep
from sys import stderr


before = datetime.now(tz=timezone.utc)
after = before - timedelta(days=14)

pages = list_all_pages('*', after=None, before=None, tags=['PEDP'], total=True)
count = next(pages)
progress = tqdm(pages, total=count)

print('\t'.join([
    'URL',
    'Status',
    'Effective Status',
    'Capture Time',
    'Latest Valid Capture Time',
    'Scanner URL'
]))

for index, page in enumerate(progress):
    # if index > 0 and index % 25 == 0:
    #     progress.write('Chilling for a moment...', file=stderr)
    #     sleep(10)

    page = add_versions_to_page(page, after=after, before=before)
    latest = next(list_page_versions(page['uuid'], None, before, chunk_size=1))
    if not len(page['versions']) and not latest:
        progress.write(f'!! No good versions for {page["uuid"]} (skipping)', file=stderr)
        continue

    latest_valid = page['versions'][0]
    latest_valid['response'] = load_url(latest_valid['body_url'], timeout=20, headers={'accept': '*/*'})
    effective_status = get_version_status(latest_valid)

    print('\t'.join([
        page['url'],
        str(latest['status']),
        str(effective_status),
        latest['capture_time'],
        latest_valid and latest_valid['capture_time'] or '',
        f'https://monitoring.envirodatagov.org/page/{page["uuid"]}'
    ]))
