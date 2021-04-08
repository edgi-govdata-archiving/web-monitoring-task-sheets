"""
Tools for outputting CSVs based on analysis data.
"""

import csv
from datetime import datetime
from pathlib import Path
import re
import sys


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
    'This Period - Side by Side',
    'Latest to Base - Side by Side',
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


def write_csv(parent_directory, name, rows):
    """
    Write a CSV to disk with rows representing `(page, analysis, error)`
    tuples.
    """
    filename = re.sub(r'[:/]', '_', name) + '.csv'
    filepath = parent_directory / filename

    timestamp = datetime.utcnow().isoformat() + 'Z'

    with filepath.open('w') as file:
        writer = csv.writer(file)
        writer.writerow(HEADERS)

        for index, [page, analysis, error] in enumerate(rows):
            writer.writerow(format_row(page, analysis, error, index, name, timestamp))


def format_row(page, analysis, error, index, name, timestamp):
    version_earliest = page['earliest']
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
        create_view_url(page, version_earliest, version_end),
        version_end['capture_time'].isoformat(),
        version_earliest['capture_time'].isoformat(),
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
            analysis['status'],
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


def clean_string(text):
    if text:
        return re.sub(r'[\n\s]+', ' ', text.strip())
    else:
        return ''


def create_view_url(page, a, b):
    a_id = a['uuid'] if a else ''
    b_id = b['uuid'] if b else ''
    return f'https://monitoring.envirodatagov.org/page/{page["uuid"]}/{a_id}..{b_id}'


def format_hash(digest):
    if digest == EMPTY_HASH or not digest:
        return '[no change]'
    return digest[:10]


def format_redirects(server_redirects, client_redirect=None):
    formatted = ' → '.join(server_redirects)
    if client_redirect:
        formatted = f' ⇥ {client_redirect}'

    return formatted
