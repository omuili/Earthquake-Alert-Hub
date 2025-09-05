from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, Mapping, Any

@dataclass
class Rule:
    id: int | None
    name: str
    min_mag: float
    bbox: Optional[str] = None 

def parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    lon1, lat1, lon2, lat2 = map(float, bbox.split(","))
    # normalize so lon1<=lon2 and lat1<=lat2
    if lon1 > lon2:
        lon1, lon2 = lon2, lon1
    if lat1 > lat2:
        lat1, lat2 = lat2, lat1
    return lon1, lat1, lon2, lat2

def quake_matches_rule(quake: Mapping[str, Any], rule: Rule) -> bool:
    """
    quake keys expected: id, time_ms, mag, place, lon, lat, depth_km
    """
    mag = float(quake.get("mag") or 0.0)
    if mag < float(rule.min_mag):
        return False
    if rule.bbox:
        lon1, lat1, lon2, lat2 = parse_bbox(rule.bbox)
        lon = float(quake["lon"]); lat = float(quake["lat"])
        if not (lon1 <= lon <= lon2 and lat1 <= lat <= lat2):
            return False
    return True
