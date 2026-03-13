from hormuz_index.config import Settings
from hormuz_index.providers.marinetraffic import normalize_response
from hormuz_index.cli import _resolve_provider


def test_normalize_marinetraffic_response() -> None:
    payload = {
        "DATA": [
            {
                "MMSI": "538009877",
                "SHIPNAME": "PACIFIC TRADER",
                "TIMESTAMP": "2026-03-13T00:40:00Z",
                "LAT": "26.12",
                "LON": "57.02",
                "SPEED": "111",
                "COURSE": "87",
                "SHIPTYPE": "70",
            }
        ],
        "METADATA": {"CURSOR": None},
    }

    events = normalize_response(payload)

    assert len(events) == 1
    assert events[0].mmsi == 538009877
    assert events[0].sog == 11.1
    assert events[0].ship_type == 70
    assert events[0].provider == "marinetraffic"


def test_resolve_provider_prefers_marinetraffic() -> None:
    settings = Settings.load()
    settings = settings.__class__(
        **{
            **settings.__dict__,
            "marinetraffic_api_key": "mt-key",
            "aisstream_api_key": "ais-key",
        }
    )

    assert _resolve_provider(settings, None) == "marinetraffic"
