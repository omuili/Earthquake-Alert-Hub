from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Optional
import httpx

FEEDS = {
    "all_hour":  "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "all_day":   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "all_week":  "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
    "all_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",

    "2.5_day":   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson",
    "2.5_week":  "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson",
    "4.5_day":   "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson",
    "significant_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson",
}

@dataclass(frozen=True)
class Quake:
    id: str
    time_ms: int           
    mag: float              
    place: str              
    lon: float
    lat: float
    depth_km: float

    def to_dict(self) -> dict:
        return asdict(self)

def _normalize_feature(feature: dict) -> Optional[Quake]:
    try:
        pid = feature["id"]
        props = feature.get("properties") or {}
        geom  = feature.get("geometry") or {}
        coords = (geom.get("coordinates") or [None, None, None])

        lon, lat, depth = coords[0], coords[1], coords[2]
        t = props.get("time")

        if t is None or lon is None or lat is None or depth is None:
            return None

        mag = props.get("mag")
        return Quake(
            id=pid,
            time_ms=int(t),
            mag=float(mag) if mag is not None else 0.0,
            place=str(props.get("place") or ""),
            lon=float(lon),
            lat=float(lat),
            depth_km=float(depth),
        )
    except Exception:
        return None

def fetch_quakes(feed: str = "all_hour",
                 timeout: float = 30.0,
                 client: Optional[httpx.Client] = None) -> List[Quake]:

    url = FEEDS.get(feed, feed)
    close_client = False
    if client is None:
        client = httpx.Client(timeout=timeout)
        close_client = True

    try:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
        feats = data.get("features", [])
        quakes: List[Quake] = []
        for f in feats:
            q = _normalize_feature(f)
            if q is not None:
                quakes.append(q)
        return quakes
    finally:
        if close_client:
            client.close()


if __name__ == "__main__":
    import sys, json
    chosen = sys.argv[1] if len(sys.argv) > 1 else "all_hour"
    qs = fetch_quakes(chosen)
    print(f"Fetched {len(qs)} quakes from {chosen}")
    for q in qs[:5]:
        print(json.dumps(q.to_dict(), indent=2))
