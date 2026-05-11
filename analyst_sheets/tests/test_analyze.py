from ..analyze import get_version_status


EMPTY_HTML = """
    <!doctype html>
    <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Hello</title>
        </head>
        <body>
        </body>
    </html>
"""


class TestGetVersionStatus:
    def test_redirects_are_ok(self):
        assert 200 == get_version_status({
            'url': 'https://hazards.fema.gov/nri/take-action',
            'status': 200,
            'source_metadata': {
                'statuses': [
                    '301',
                    '200'
                ],
                'redirects': [
                    'https://hazards.fema.gov/nri/take-action',
                    'https://www.fema.gov/emergency-managers/practitioners/resilience-analysis-and-planning-tool',
                ],
                'redirected_url': 'https://www.fema.gov/emergency-managers/practitioners/resilience-analysis-and-planning-tool',
            },
            'title': 'Resilience Analysis and Planning Tool (RAPT) | FEMA.gov',
            'headers': {},
            '_body_text': EMPTY_HTML,
        })

    def test_redirects_to_root_path_are_404(self):
        # Redirect to "/".
        assert 404 == get_version_status({
            'url': 'https://waterdata.usgs.gov/nwis',
            'status': 200,
            'source_metadata': {
                'statuses': [
                    '301',
                    '200'
                ],
                'redirects': [
                    'https://waterdata.usgs.gov/nwis',
                    'https://waterdata.usgs.gov/',
                ],
                'redirected_url': 'https://waterdata.usgs.gov/',
            },
            'title': 'USGS Water Data for the Nation',
            'headers': {},
            '_body_text': EMPTY_HTML,
        })

        # Redirect to root-like path.
        assert 404 == get_version_status({
            'url': 'https://eta.lbl.gov/justice-40',
            'status': 200,
            'source_metadata': {
                'statuses': [
                    '301',
                    '200'
                ],
                'redirects': [
                    'https://eta.lbl.gov/justice-40',
                    'https://eta.lbl.gov/home',
                ],
                'redirected_url': 'https://eta.lbl.gov/home',
            },
            'title': 'Energy Technologies Area | Energy Technologies Area',
            'headers': {},
            '_body_text': EMPTY_HTML,
        })

    def test_redirects_to_root_across_domains_are_ok(self):
        # Redirect to "/".
        assert 200 == get_version_status({
            'url': 'https://waterdata.usgs.gov/nwis',
            'status': 200,
            'source_metadata': {
                'statuses': [
                    '301',
                    '200'
                ],
                'redirects': [
                    'https://waterdata.usgs.gov/nwis',
                    'https://www.usgs.gov/',
                ],
                'redirected_url': 'https://www.usgs.gov/',
            },
            'title': 'USGS Water Data for the Nation',
            'headers': {},
            '_body_text': EMPTY_HTML,
        })
