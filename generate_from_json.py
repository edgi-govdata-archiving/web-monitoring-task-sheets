# TODO: This should really be merged in with `generate_task_sheets.py` as a
# secondary command or something.

import gzip
import json
from pathlib import Path
import sys
from web_monitoring.db import DbJsonDecoder
from generate_task_sheets import ResultItem, filter_priority, write_sheets


def read_json_data(input: Path) -> list[ResultItem]:
    with input.open('rb') as file:
        start = file.read(2)
        file.seek(0)
        if start == b'\x1f\x8b':
            file = gzip.open(file)

        raw_data = json.load(file, cls=DbJsonDecoder)

    return [
        (
            item['page'],
            item['analysis'],
            Exception(item['error']) if item['error'] else None,
        )
        for item in raw_data
    ]


def main(input: Path, output: Path, tags: list[str], threshold: float) -> None:
    data = read_json_data(input)

    if tags:
        data = [
            item for item in data
            if any(
                tag['name'] in tags
                for tag in item[0]['tags']
            )
        ]

    output.mkdir(exist_ok=True)
    write_sheets(output, filter_priority(data, threshold))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Re-generate CSVs from the raw data of a previous run.')
    parser.add_argument('input_json', type=Path,
                        help='Path to JSON data to reformat as CSV.')
    parser.add_argument('--output', type=Path, required=True,
                        help='Output CSV files in this directory.')
    parser.add_argument('--tag', action='append', help='Only anlyze pages with this tag (repeat for multiple tags).')
    parser.add_argument('--threshold', type=float, default=0.0, help='Minimum priority value to include in output.')
    options = parser.parse_args()

    input_path: Path = options.input_json
    if input_path.is_dir():
        for name in ['_results.json', '_results.json.gz']:
            candidate = input_path / name
            if candidate.is_file():
                input_path = candidate
                break
    if not input_path.is_file():
        print(f'Cannot find input JSON file at "{input_path}"')
        sys.exit(1)

    main(
        input=input_path,
        output=options.output,
        tags=options.tag,
        threshold=options.threshold
    )
