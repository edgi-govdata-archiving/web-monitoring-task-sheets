"""
Tools for outputting CSVs based on analysis data.
"""

import csv
from datetime import datetime, timezone
from pathlib import Path
import re
from surt import surt


EMPTY_HASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'


HEADERS = [
    'Index',
    'Version',
    'Output Date/Time',
    'Maintainers',
    'Site Name',
    'Page Title',
    'URL',
    '---',
    'Scanner Comparison',
    'IA Comparison',
    'Date Found - Latest',
    'Date Found - Base',
    'Diff Length',
    'Diff Hash',
    'Text Diff Length',
    'Text Diff Hash',
    '# versions',
    'Priority',

    'Error',

    'Home page?',
    'Changed status?',
    'Effective Status',
    'Status',
    'Readable?',
    'Key Terms',
    '% Changed Text',
    'Longest Text Change',
    'Links diff hash',
    'Links changes',
    '% Changed Links',
    'Removed link to self',
    'Client Redirect?',
    'Redirects Changed?',
    'Prior Redirects',
    'Current Redirects'
]


def write_csv(parent_directory: Path, name: str, results, deep):
    """
    Write a CSV to disk with rows representing found changes. If ``deep`` is
    true, write a row for every change found in each result. Otherwise, write
    one row for the overall analysis in each result.
    """
    filename = re.sub(r'[:/]', '_', name) + '.csv'
    filepath = parent_directory / filename

    timestamp = datetime.now(timezone.utc).isoformat() + 'Z'

    with filepath.open('w') as file:
        writer = csv.writer(file)
        writer.writerow(HEADERS)

        for index, result in enumerate(results):
            row_number = index + 1
            writer.writerow(format_row(
                result.page,
                result.timeframe,
                result.overall,
                result.error,
                row_number,
                name,
                timestamp,
                overall=deep,
            ))
            if deep:
                for change_index, change in enumerate(result.changes):
                    row_number = f'{index + 1}-{change_index + 1}'
                    analysis = change['analysis']
                    timeframe = change['versions']
                    writer.writerow(format_row(
                        result.page,
                        timeframe,
                        analysis,
                        None,
                        row_number,
                        name,
                        timestamp,
                        overall=False,
                    ))

                # Blank row to help separate page groups in deep analysis.
                writer.writerow(['---' for _ in HEADERS])


def format_row(page, timeframe, analysis, error, index, name, timestamp, overall: bool):
    version_start = timeframe[-1]
    version_end = timeframe[0]

    row = [
        index,
        'OVERALL' if overall else version_end['uuid'],
        timestamp,
        ', '.join(m['name'] for m in page['maintainers']),
        name,
        clean_string(page['title']),
        page['url'],
        '',
        create_view_url(page, version_start, version_end),
        create_ia_changes_url(page, version_start, version_end),
        version_end['capture_time'].isoformat(),
        version_start['capture_time'].isoformat(),
    ]

    if analysis:
        row.extend([
            analysis['source']['diff_length'],
            format_hash(analysis['source']['diff_hash']),
            analysis['text']['diff_length'],
            format_hash(analysis['text']['diff_hash']),
            # Placeholder column: version count is no longer relevant.
            '',
            format(analysis['priority'], '.3f'),
            # Error column is empty in this case. :)
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
            '',
            '?',
            str(error)
        ])

    return row


def clean_string(text):
    if text:
        return re.sub(r'[\n\s]+', ' ', text.strip())
    else:
        return ''


def create_view_url(page, a, b):
    a_id = a['uuid'] if a else ''
    b_id = b['uuid'] if b else ''
    return f'https://monitoring.envirodatagov.org/page/{page["uuid"]}/{a_id}..{b_id}'


def create_ia_changes_url(page, a, b) -> str:
    if (
        a and b
        and a['source_type'] == 'internet_archive'
        and b['source_type'] == 'internet_archive'
        and surt(a['url'], reverse_ipaddr=False) == surt(b['url'], reverse_ipaddr=False)
    ):
        a_time = ia_timestamp(a["capture_time"])
        b_time = ia_timestamp(b["capture_time"])
        return f'https://web.archive.org/web/diff/{a_time}/{b_time}/{b["url"]}'
    else:
        return ''


def ia_timestamp(datetime):
    return datetime.strftime('%Y%m%d%H%M%S')


def format_hash(digest):
    if digest == EMPTY_HASH or not digest:
        return '[no change]'
    return digest[:10]


def format_redirects(server_redirects, client_redirect=None):
    formatted = ' → '.join(server_redirects)
    if client_redirect:
        formatted = f' ⇥ {client_redirect}'

    return formatted
