import json
from pathlib import Path
import pytest
from web_monitoring.db import DbJsonDecoder
from generate_task_sheets import maybe_bad_capture


# These tests for `maybe_bad_capture` expect a directory full of JSON files
# representing the API server's responses for version records that should
# qualify as good or bad captures.
# They should be in subdirectories named `*-good` if they are good captures and
# `*-bad` if they are bad captures.
FIXTURES_PATH = Path(__file__).parent / 'analyst_sheets' / 'tests' / 'fixtures' / 'bad_capture'
# Versions that currently fail but should not fail the build (for now).
XFAIL_FILES = [
    # No great way to identify this without access to the body.
    'b47ca1d6-0f4e-4015-9940-dc666f755eb1.json',
]


@pytest.mark.parametrize('file,expected', [
    pytest.param(
        f'{file.parent.name}/{file.name}',
        file.parent.name.endswith('-bad'),
        marks=pytest.mark.xfail if file.name in XFAIL_FILES else []
    )
    for file in FIXTURES_PATH.glob('*/*.json')
])
def test_maybe_bad_capture(file, expected):
    with (FIXTURES_PATH / file).open() as file:
        version = json.load(file, cls=DbJsonDecoder)['data']
    assert expected == maybe_bad_capture(version)
