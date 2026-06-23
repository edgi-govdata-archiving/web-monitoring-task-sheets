"""
Tools for outputting CSVs based on analysis data.
"""

import csv
from datetime import datetime, timezone
from pathlib import Path
import re
from surt import surt
from typing import Any
from .analyze import get_redirects, url_change_type


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
    '---',  # Formerly "# Versions", no longer relevant.
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

    timestamp = format_datetime(datetime.now(timezone.utc))

    with filepath.open('w') as file:
        writer = csv.DictWriter(file, HEADERS)
        writer.writeheader()

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
                writer.writerow({key: '---' for key in HEADERS})


def format_row(page, timeframe, analysis, error, index, name, timestamp, overall: bool):
    version_start = timeframe[-1]
    version_end = timeframe[0]

    row = {
        'Index': index,
        'Version': 'OVERALL' if overall else version_end['uuid'],
        'Output Date/Time': timestamp,
        'Maintainers': ', '.join(m['name'] for m in page['maintainers']),
        'Site Name': name,
        'Page Title': clean_string(page['title']),
        'URL': page['url'],
        'Scanner Comparison': create_view_url(page, version_start, version_end),
        'IA Comparison': create_ia_changes_url(page, version_start, version_end),
        'Date Found - Latest': format_datetime(version_end['capture_time']),
        'Date Found - Base': format_datetime(version_start['capture_time']),
    }

    if analysis:
        row.update({
            'Priority': format(analysis['priority'], '.3f'),
            'Home page?': analysis['root_page'],
            'Changed status?': analysis['status_changed'],
            'Effective Status': format_status(analysis['status_b']),
            'Status': format_status(version_end['status']),
            'Client Redirect?': analysis['redirect']['is_client_redirect'] or '',
            'Redirects Changed?': analysis['redirect']['change_type'] or '',
            'Prior Redirects': format_redirects(analysis['redirect']['a_server'], analysis['redirect']['a_client']),
            'Current Redirects': format_redirects(analysis['redirect']['b_server'], analysis['redirect']['b_client']),
        })

        if analysis.get('source'):
            row.update({
                'Diff Length': analysis['source']['diff_length'],
                'Diff Hash': format_hash(analysis['source']['diff_hash']),
            })

        if analysis.get('text'):
            row.update({
                'Text Diff Length': analysis['text']['diff_length'],
                'Text Diff Hash': format_hash(analysis['text']['diff_hash']),
                'Readable?': analysis['text']['readable'],
                'Key Terms': ', '.join((f'{term}: {count}' for term, count in analysis['text']['key_terms'].items())),
                '% Changed Text': format(analysis['text']['percent_changed'], '.3f'),
                'Longest Text Change': analysis['text']['diff_max_length'],
            })

        if analysis.get('links'):
            row.update({
                'Links diff hash': format_hash(analysis['links']['diff_hash']),
                'Links changes': analysis['links']['diff_length'],
                '% Changed Links': format(analysis['links']['diff_ratio'], '.3f'),
                'Removed link to self': analysis['links']['removed_self_link'],
            })
    else:
        row.update({
            'Priority': '?',
            'Error': str(error),
        })

    return row


def write_redirect_change_summary(output_path: Path, results: list[tuple[str, str, Any]]):
    with output_path.open('w') as file:
        writer = csv.writer(file)
        writer.writerow([
            'Scanner',
            'Category',
            'Domain',
            'Status',
            'Redirect Type',
            'Monitored URL',
            'Redirect Old',
            'Redirect New',
            'All Redirects Old',
            'All Redirects New',
        ])
        for category, domain, result in results:
            analysis = result.overall and result.overall['redirect']
            if not analysis or not analysis['change_type']:
                continue

            change_type = analysis['change_type']
            a = result.timeframe[-1]
            b = result.timeframe[0]
            a_all = [
                url
                for url in [*analysis['a_server'], analysis['a_client']]
                if url
            ]
            b_all = [
                url
                for url in [*analysis['b_server'], analysis['b_client']]
                if url
            ]
            a_url = a_all[-1] if len(a_all) else a['url']
            b_url = b_all[-1] if len(b_all) else b['url']

            writer.writerow([
                create_view_url(result.page, a, b),
                category,
                domain,
                format_status(result.overall['status_b']),
                change_type,
                b['url'],
                a_url,
                b_url,
                format_redirects(analysis['a_server'], analysis['a_client']),
                format_redirects(analysis['b_server'], analysis['b_client']),
            ])


def write_redirect_current_summary(output_path: Path, results: list[tuple[str, str, Any]]):
    with output_path.open('w') as file:
        writer = csv.writer(file)
        writer.writerow([
            'Scanner',
            'Category',
            'Domain',
            'Status',
            'Redirect Type',
            'Monitored URL',
            'Redirected URL',
            'All Redirects',
        ])
        for category, domain, result in results:
            if not result.overall:
                continue

            a = result.timeframe[-1]
            b = result.timeframe[0]
            redirects, redirect_server, redirect_client = get_redirects(b)
            if not redirects:
                continue

            change_type = url_change_type(b['url'], redirects[-1])
            if not change_type:
                continue

            writer.writerow([
                create_view_url(result.page, a, b),
                category,
                domain,
                format_status(result.overall['status_b']),
                change_type,
                b['url'],
                redirects[-1],
                format_redirects(redirect_server, redirect_client),
            ])


def dig(container, *keys, default: Any = None) -> Any:
    value = container
    for key in keys:
        try:
            value = value[key]
        except LookupError:
            value = None
        if value is None:
            return default

    return value


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


def format_datetime(value: datetime | None) -> str:
    if value is None:
        return ''

    value = value.replace(microsecond=0)
    return re.sub(r'\+00:00$', 'Z', value.isoformat())


def format_hash(digest):
    if digest == EMPTY_HASH or not digest:
        return '[no change]'
    return digest[:10]


def format_redirects(server_redirects, client_redirect=None):
    formatted = ' → '.join(server_redirects)
    if client_redirect:
        formatted = f' ⇥ {client_redirect}'

    return formatted


def format_status(value: str | int | None) -> str:
    if value is None or value == 600 or value == '':
        return '(offline)'

    return str(value)
