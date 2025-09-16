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
for index, page in enumerate(progress):
    # if index > 0 and index % 25 == 0:
    #     progress.write('Chilling for a moment...', file=stderr)
    #     sleep(10)

    page = add_versions_to_page(page, after=after, before=before)
    if not len(page['versions']):
        progress.write(f'No good versions in time range for {page["uuid"]}', file=stderr)
        page['versions'] = [next(list_page_versions(page['uuid'], None, before, chunk_size=1))]
    if not len(page['versions']):
        progress.write(f'!! No good versions at all for {page["uuid"]} (skipping)', file=stderr)
        continue

    latest = page['versions'][0]
    latest['response'] = load_url(latest['body_url'], timeout=20, headers={'accept': '*/*'})
    effective_status = get_version_status(latest)
    scanner_url = f'https://monitoring.envirodatagov.org/page/{page["uuid"]}'
    print(f'{page["url"]}\t{latest["status"]}\t{effective_status}\t{scanner_url}')
