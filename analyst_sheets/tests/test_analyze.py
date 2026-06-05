import pytest
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


def dummy_version(urls: list[tuple[int, str]], **kwargs) -> dict:
    return {
        'url': urls[0][1],
        'status': urls[-1][0],
        'headers': {},
        '_body_text': EMPTY_HTML,
        **kwargs,
        'source_metadata': {
            'statuses': [u[0] for u in urls],
            'redirects': [u[1] for u in urls],
            'redirected_url': urls[-1][1],
            **kwargs.get('source_metadata', {}),
        },
    }


class TestGetVersionStatus:
    def test_redirects_are_ok(self):
        assert 200 == get_version_status(dummy_version(
            urls=[
                (301, 'https://hazards.fema.gov/nri/take-action'),
                (200, 'https://www.fema.gov/emergency-managers/practitioners/resilience-analysis-and-planning-tool'),
            ],
            title='Resilience Analysis and Planning Tool (RAPT) | FEMA.gov',
        ))

    def test_redirects_to_root_path_are_404(self):
        # Redirect to "/".
        assert 404 == get_version_status(dummy_version(
            urls=[
                (301, 'https://waterdata.usgs.gov/nwis'),
                (200, 'https://waterdata.usgs.gov/'),
            ],
            title='USGS Water Data for the Nation',
        ))

        # Redirect to root-like path.
        assert 404 == get_version_status(dummy_version(
            urls=[
                (301, 'https://eta.lbl.gov/justice-40'),
                (200, 'https://eta.lbl.gov/home'),
            ],
            title='Energy Technologies Area | Energy Technologies Area',
        ))

    def test_redirects_to_root_across_domains_are_ok(self):
        # Redirect to "/".
        assert 200 == get_version_status(dummy_version(
            urls=[
                (301, 'https://waterdata.usgs.gov/nwis'),
                (200, 'https://www.usgs.gov/'),
            ],
            title='USGS Water Data for the Nation',
        ))

    def test_special_epa_signpost(self):
        # https://api.monitoring.envirodatagov.org/api/v0/versions/00002893-4493-4db4-8f9d-6f4644bb99c5
        assert 404 == get_version_status(dummy_version(
            urls=[
                (302, 'https://www3.epa.gov/climatechange/kids/solutions/index.html'),
                (200, 'https://www.epa.gov/sites/production/files/signpost/cc.html'),
            ],
            title='Help finding information | US EPA',
        ))

    def test_special_nasa_climate(sefl):
        # https://api.monitoring.envirodatagov.org/api/v0/versions/02204f0a-ae18-4bc7-a799-245e21028a10
        assert 404 == get_version_status(dummy_version(
            urls=[
                (302, 'https://climate.nasa.gov/explore/ask-nasa-climate/183/the-year-without-a-summer/'),
                (200, 'https://science.nasa.gov/climate-change/')
            ],
            title='Climate Change - NASA Science',
        ))

    @pytest.mark.xfail
    def test_special_federal_register_unblock(self):
        # https://api.monitoring.envirodatagov.org/api/v0/versions/bcf36392-8b37-4dcc-b4a2-9f62c5effb18
        assert 429 == get_version_status(dummy_version(
            urls=[
                (302, 'https://www.federalregister.gov/documents/2021/07/27/2021-15122/national-oil-and-hazardous-substances-pollution-contingency-plan-monitoring-requirements-for-use-of'),
                (200, 'https://unblock.federalregister.gov/'),
            ],
            title='USGS Water Data for the Nation',
        ))
