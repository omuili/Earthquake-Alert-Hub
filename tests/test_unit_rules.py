from app.rules import Rule, quake_matches_rule

def test_min_mag_only():
    r = Rule(id=1, name="M3+", min_mag=3.0, bbox=None)
    q = {"mag": 3.1, "lon": -120.0, "lat": 35.0}
    assert quake_matches_rule(q, r)
    q["mag"] = 2.9
    assert not quake_matches_rule(q, r)

def test_bbox_filter():
    r = Rule(id=2, name="West", min_mag=2.0, bbox="-125,32,-114,42")
    inside = {"mag": 2.1, "lon": -120.0, "lat": 35.0}
    outside = {"mag": 4.0, "lon": -100.0, "lat": 40.0}
    assert quake_matches_rule(inside, r)
    assert not quake_matches_rule(outside, r)
