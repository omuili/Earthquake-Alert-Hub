import respx
from httpx import Response
from fastapi.testclient import TestClient

from app.main import app
from app.usgs import FEEDS

client = TestClient(app)

USGS_SAMPLE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "us123",
            "properties": {"time": 1700000000000, "mag": 3.2, "place": "10km W of Test, CA"},
            "geometry": {"type": "Point", "coordinates": [-121.5, 37.5, 10.0]},
        },
        {
            "type": "Feature",
            "id": "us999",
            "properties": {"time": 1700000100000, "mag": 2.4, "place": "Somewhere"},
            "geometry": {"type": "Point", "coordinates": [-100.0, 40.0, 5.0]},
        },
    ],
}

def test_create_rule_and_ingest_triggers_alert():
    # Create a rule via the API
    r = client.post("/rules", data={"name": "CA 3+", "min_mag": 3.0, "bbox": "-125,32,-114,42"})
    assert r.status_code == 200
    created_id = r.json()["created"]

    # Mock the USGS feed used by /ingest (all_hour)
    with respx.mock:
        respx.get(FEEDS["all_hour"]).mock(return_value=Response(200, json=USGS_SAMPLE))
        res = client.post("/ingest", data={"feed": "all_hour"})
        assert res.status_code == 200
        data = res.json()
        assert data["ingested"] == 2

        # Verify that OUR rule produced exactly one alert for the CA quake (us123)
        mine = [a for a in data["alerts"] if a["rule_id"] == created_id and a["quake_id"] == "us123"]
        assert len(mine) == 1
