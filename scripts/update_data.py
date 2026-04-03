import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

BASE = "https://opendataapi.dmi.dk/v1/forecastedr"
COL = "harmonie_ig_sf"

LOCATION_NAME = "TASIILAQ"
DATA_FILE = Path("data.json")
HISTORY_FILE = Path("history.json")
SUMMARY_FILE = Path("summary.json")

CONNECT_TIMEOUT = 8
READ_TIMEOUT = 40
REQUEST_SLEEP = 0.35
REQUEST_RETRIES = 2
MAX_WORKERS = 1

TIME_TOLERANCE_HOURS = 2.75
HISTORY_KEEP = 144

ICE_PRESSURE_NORMAL_HPA = 1013.25

CYCLONE_TRIGGER_ZONE = {
    "lat_min": 60.0,
    "lat_max": 66.0,
    "lon_min": -35.0,
    "lon_max": -20.0,
}

LP_FAVORED_SECTOR = {
    "core_left": 330.0,
    "core_right": 30.0,
    "good_left": 300.0,
    "good_right": 60.0,
}

ICE_POINTS = [
    {"name": "source", "lon": -42.4, "lat": 69.0},
    {"name": "mid", "lon": -41.3, "lat": 68.6},
    {"name": "mouth", "lon": -40.3, "lat": 68.2},
]

EXPECTED_DRAINAGE_BEARING = 158.0

SEA_POINTS = [
    {"name": "W1", "lon": -34.9, "lat": 62.8},
    {"name": "W2", "lon": -33.7, "lat": 63.8},
    {"name": "W3", "lon": -32.5, "lat": 64.8},
    {"name": "C1", "lon": -30.5, "lat": 64.6},
    {"name": "C2", "lon": -31.1, "lat": 65.4},
    {"name": "M1", "lon": -30.1, "lat": 63.9},
    {"name": "M2", "lon": -31.8, "lat": 64.3},
    {"name": "M3", "lon": -30.2, "lat": 65.0},
    {"name": "E1", "lon": -29.1, "lat": 65.8},
    {"name": "E2", "lon": -27.5, "lat": 66.4},
]

COAST_POINTS = [
    {"name": "K1", "lon": -38.8, "lat": 65.7},
    {"name": "K2", "lon": -37.6, "lat": 66.5},
]

WEST_NAMES = ["W1", "W2", "W3"]
MID_NAMES = ["C1", "C2", "M1", "M2", "M3"]
EAST_NAMES = ["E1", "E2"]
ALL_STRAIT_NAMES = WEST_NAMES + MID_NAMES + EAST_NAMES

ICE_PARAMS_CORE = [
    "pressure-sealevel",
    "temperature-2m",
    "wind-speed-100m",
    "wind-direction-100m",
    "total-cloud-cover",
]
ICE_PARAMS = ICE_PARAMS_CORE
SEA_PARAMS = ["pressure-sealevel", "temperature-2m"]

# Optional extra-path retained for future experiments, but not used in build_payload
ICE_PARAMS_EXTRA = ["total-cloud-cover"]
EXTRA_REQUEST_SLEEP = 0.6
EXTRA_REQUEST_RETRIES = 1
ENABLE_BONUS_FETCH = False

# DMI hygiene controls
MAX_REQUESTS_PER_RUN = int(os.getenv("PITERAQ_MAX_REQUESTS", "45"))
MAX_RATE_LIMIT_HITS = int(os.getenv("PITERAQ_MAX_429", "2"))
FALLBACK_MAX_REQUESTS_PER_RUN = int(os.getenv("PITERAQ_FALLBACK_MAX_REQUESTS", "8"))
FALLBACK_MAX_RATE_LIMIT_HITS = int(os.getenv("PITERAQ_FALLBACK_MAX_429", "1"))
BACKFILL_MAX_INSTANCES = 2
BACKFILL_REQUEST_RETRIES = 1
INSTANCE_CHECK_CONNECT_TIMEOUT = float(os.getenv("PITERAQ_INSTANCE_CHECK_CONNECT_TIMEOUT", "5"))
INSTANCE_CHECK_READ_TIMEOUT = float(os.getenv("PITERAQ_INSTANCE_CHECK_READ_TIMEOUT", "8"))
EXTRA_PROBE_CONNECT_TIMEOUT = float(os.getenv("PITERAQ_EXTRA_PROBE_CONNECT_TIMEOUT", "4"))
EXTRA_PROBE_READ_TIMEOUT = float(os.getenv("PITERAQ_EXTRA_PROBE_READ_TIMEOUT", "6"))
EXTRA_PROBE_SLEEP = float(os.getenv("PITERAQ_EXTRA_PROBE_SLEEP", "0.2"))

SEA_POINTS_BACKFILL = [
    {"name": "W3", "lon": -32.5, "lat": 64.8},
    {"name": "C2", "lon": -31.1, "lat": 65.4},
    {"name": "M2", "lon": -31.8, "lat": 64.3},
]

_global_rate_limit_hits = 0
_global_request_count = 0
_global_request_budget = MAX_REQUESTS_PER_RUN
_global_rate_limit_threshold = MAX_RATE_LIMIT_HITS


def reset_request_counters(max_requests=None, max_rate_limit_hits=None):
    global _global_rate_limit_hits, _global_request_count, _global_request_budget, _global_rate_limit_threshold
    _global_rate_limit_hits = 0
    _global_request_count = 0
    _global_request_budget = MAX_REQUESTS_PER_RUN if max_requests is None else max_requests
    _global_rate_limit_threshold = MAX_RATE_LIMIT_HITS if max_rate_limit_hits is None else max_rate_limit_hits


TREND_TOLERANCES = {
    "h6": timedelta(hours=2.0),
    "h12": timedelta(hours=2.5),
    "h24": timedelta(hours=3.0),
    "h72": timedelta(hours=6.0),
}


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def norm(x, lo, hi):
    if x is None or hi == lo:
        return 0.0
    if hi > lo:
        return clamp((x - lo) / (hi - lo), 0.0, 1.0)
    return clamp((lo - x) / (lo - hi), 0.0, 1.0)


def avg(values):
    vals = [v for v in values if is_num(v)]
    return sum(vals) / len(vals) if vals else None


def kelvin_to_celsius(k):
    return k - 273.15


def parse_iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def iso_z(dt):
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def now_utc():
    return datetime.now(timezone.utc)


def now_utc_str():
    return now_utc().strftime("%Y-%m-%d %H:%M UTC")


def is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def safe_get(arr, idx):
    if not isinstance(arr, list):
        return None
    if idx < 0 or idx >= len(arr):
        return None
    return arr[idx]


def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fmt_msg_num(x, digits=1, signed=False, none_text="NA"):
    if not is_num(x):
        return none_text
    return f"{x:+.{digits}f}" if signed else f"{x:.{digits}f}"


def compact_score_tag(prefix, value, uncertain=False):
    if not is_num(value):
        return f"{prefix}?"
    iv = int(round(value))
    return f"{prefix}{iv}{'?' if uncertain else ''}"


def compact_temp_trend_tag(prefix, value):
    if not is_num(value):
        return f"{prefix}?"
    iv = int(round(value))
    return f"{prefix}{iv:+d}"


def compact_gate_tag(prefix, value):
    if not is_num(value):
        return f"{prefix}?"
    return f"{prefix}{int(round(value))}"


def normalize_angle(angle):
    return angle % 360.0


def signed_shortest_angle_diff(a, b):
    d = (a - b + 180.0) % 360.0 - 180.0
    return d


def smallest_angle_to_north(angle):
    a = normalize_angle(angle)
    return min(a, 360.0 - a)


def point_in_wrapped_sector(angle, left_deg, right_deg):
    a = normalize_angle(angle)
    left = normalize_angle(left_deg)
    right = normalize_angle(right_deg)
    if left <= right:
        return left <= a <= right
    return a >= left or a <= right


def bearing_deg(from_lon, from_lat, to_lon, to_lat):
    if not all(is_num(v) for v in [from_lon, from_lat, to_lon, to_lat]):
        return None

    phi1 = math.radians(from_lat)
    phi2 = math.radians(to_lat)
    dlon = math.radians(to_lon - from_lon)

    y = math.sin(dlon) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlon)
    brng = math.degrees(math.atan2(y, x))
    return normalize_angle(brng)


def haversine_km(lon1, lat1, lon2, lat2):
    if not all(is_num(v) for v in [lon1, lat1, lon2, lat2]):
        return None
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def lp_sector_score(bearing):
    if not is_num(bearing):
        return 0.0
    if point_in_wrapped_sector(bearing, 330.0, 30.0):
        dist = smallest_angle_to_north(bearing)
        return clamp(100.0 - (dist / 30.0) * 20.0, 80.0, 100.0)
    if point_in_wrapped_sector(bearing, 300.0, 60.0):
        dist = max(0.0, smallest_angle_to_north(bearing) - 30.0)
        return clamp(80.0 - (dist / 30.0) * 30.0, 50.0, 80.0)
    if point_in_wrapped_sector(bearing, 240.0, 120.0):
        south_dist = abs(abs(signed_shortest_angle_diff(bearing, 180.0)))
        return clamp(35.0 - norm(south_dist, 0.0, 60.0) * 35.0, 0.0, 35.0)
    return 20.0


def lp_alignment_label(bearing):
    if not is_num(bearing):
        return "unknown"
    if point_in_wrapped_sector(bearing, 330.0, 30.0):
        return "core"
    if point_in_wrapped_sector(bearing, 300.0, 60.0):
        return "favored"
    if point_in_wrapped_sector(bearing, 240.0, 120.0):
        return "side"
    return "south"


def angular_distance_to_sector(angle, left_deg, right_deg):
    if not is_num(angle):
        return None
    if point_in_wrapped_sector(angle, left_deg, right_deg):
        return 0.0
    a = normalize_angle(angle)
    left = normalize_angle(left_deg)
    right = normalize_angle(right_deg)
    return min(abs(signed_shortest_angle_diff(a, left)), abs(signed_shortest_angle_diff(a, right)))


def lp_distance_to_favored_sector(bearing):
    return angular_distance_to_sector(bearing, LP_FAVORED_SECTOR["good_left"], LP_FAVORED_SECTOR["good_right"])


def lp_distance_score(lpd_deg):
    if not is_num(lpd_deg):
        return 0.0
    return clamp(100.0 * (1.0 - norm(lpd_deg, 0.0, 30.0)), 0.0, 100.0)


def compass4_from_bearing(bearing):
    if not is_num(bearing):
        return "?"
    a = normalize_angle(bearing)
    if a >= 315 or a < 45:
        return "N"
    if a < 135:
        return "E"
    if a < 225:
        return "S"
    return "W"


def lp_motion_cardinal(prev_lon, prev_lat, now_lon, now_lat):
    move_bearing = bearing_deg(prev_lon, prev_lat, now_lon, now_lat)
    return compass4_from_bearing(move_bearing), move_bearing


def lp_trend_toward_favored_sector(now_bearing, prev_bearing):
    if not (is_num(now_bearing) and is_num(prev_bearing)):
        return {
            "distance_now": None,
            "distance_prev": None,
            "center_distance_now": None,
            "center_distance_prev": None,
            "toward_deg": None,
            "approaching": False,
        }

    now_dist = lp_distance_to_favored_sector(now_bearing)
    prev_dist = lp_distance_to_favored_sector(prev_bearing)
    now_center = smallest_angle_to_north(now_bearing)
    prev_center = smallest_angle_to_north(prev_bearing)

    if now_dist == 0.0 and prev_dist == 0.0:
        toward = prev_center - now_center
    else:
        toward = prev_dist - now_dist

    return {
        "distance_now": now_dist,
        "distance_prev": prev_dist,
        "center_distance_now": now_center,
        "center_distance_prev": prev_center,
        "toward_deg": toward,
        "approaching": is_num(toward) and toward >= 2.0,
    }


def format_lpv_label(toward_deg, motion_cardinal):
    if not is_num(toward_deg):
        return "LPV?"
    sign = "+" if toward_deg >= 0 else "-"
    cardinal = motion_cardinal if motion_cardinal in {"N", "E", "S", "W"} else "?"
    return f"LPV{sign}{int(round(abs(toward_deg)))}{cardinal}"


def lp_gate_factor(lpd_deg, lpv_toward_deg):
    if not is_num(lpd_deg):
        return 0.35

    dist_factor = 1.0 - norm(lpd_deg, 0.0, 30.0)

    if is_num(lpv_toward_deg):
        if lpv_toward_deg >= 6:
            trend_factor = 1.00
        elif lpv_toward_deg >= 2:
            trend_factor = 0.80
        elif lpv_toward_deg > -2:
            trend_factor = 0.55
        elif lpv_toward_deg > -6:
            trend_factor = 0.30
        else:
            trend_factor = 0.15
    else:
        trend_factor = 0.45

    return clamp(0.15 + 0.85 * dist_factor * trend_factor, 0.15, 1.0)


def lp_geometry_score(bearing, lpd_deg, lpv_toward_deg, approach_speed_deg6h=None):
    pos_score = lp_sector_score(bearing)
    dist_score = lp_distance_score(lpd_deg)
    if is_num(lpv_toward_deg):
        trend_score = 100.0 * norm(lpv_toward_deg, -10.0, 10.0)
    else:
        trend_score = 50.0
    if is_num(approach_speed_deg6h):
        approach_score = 100.0 * norm(approach_speed_deg6h, -8.0, 8.0)
    else:
        approach_score = trend_score
    geom = 0.42 * pos_score + 0.30 * dist_score + 0.18 * trend_score + 0.10 * approach_score
    return clamp(geom, 0.0, 100.0)


def sanitize_phase_text(phase):
    phase = str(phase or "UNKNOWN").strip()
    while phase.startswith("STALE (STALE ("):
        phase = "STALE (" + phase[len("STALE (STALE ("):]
    while phase.endswith("))"):
        phase = phase[:-1]
    return phase


def error_to_text(error):
    if isinstance(error, BaseException):
        msg = str(error).strip()
        return f"{type(error).__name__}: {msg}" if msg else type(error).__name__
    if error is None:
        return "UnknownError"
    return f"NonExceptionError: {type(error).__name__}: {error}"


def sanitize_fetch_errors(fetch_errors):
    if not isinstance(fetch_errors, dict):
        return {}

    cleaned = {}
    for key, value in fetch_errors.items():
        key_txt = str(key)
        if isinstance(value, dict):
            nested = sanitize_fetch_errors(value)
            if nested:
                cleaned[key_txt] = nested
            continue

        txt = value if isinstance(value, str) else error_to_text(value)
        if "exceptions must derive from BaseException" in txt:
            txt = "LegacyNonExceptionRaise: older code attempted to raise a non-Exception object"
        cleaned[key_txt] = txt

    return cleaned


def snapshot_gap_hours(current_snapshot, reference_snapshot):
    if not isinstance(current_snapshot, dict) or not isinstance(reference_snapshot, dict):
        return None
    try:
        now_dt = parse_iso(current_snapshot["t"])
        ref_dt = parse_iso(reference_snapshot["t"])
    except Exception:
        return None
    return abs((now_dt - ref_dt).total_seconds()) / 3600.0


def trend_gap_within_expected(gap_hours, expected_hours, allowed_slip_hours):
    if not is_num(gap_hours):
        return False
    return abs(gap_hours - expected_hours) <= allowed_slip_hours


def quality_level(score):
    if not is_num(score):
        return "LOW"
    if score >= 80:
        return "HIGH"
    if score >= 55:
        return "MED"
    return "LOW"


def quality_color(score):
    if not is_num(score):
        return "ORG"
    if score >= 80:
        return "GRN"
    if score >= 55:
        return "YEL"
    return "ORG"


def build_quality_tag(level, prefix):
    level = str(level or "LOW").upper()
    return f"{prefix}:{level[:1]}"


def annotate_payload_quality(payload):
    if not isinstance(payload, dict):
        return payload

    meta = payload.setdefault("meta", {})
    derived = payload.setdefault("derived", {})
    output = payload.setdefault("output", {})
    quality_flags = derived.get("qualityFlags", [])
    if not isinstance(quality_flags, list):
        quality_flags = []

    stale = bool(meta.get("stale"))
    trend = str(derived.get("trendDataStatus") or "")

    ice_p = derived.get("usedIcePressurePoints")
    ice_t = derived.get("usedIceTempPoints")
    ice_w = derived.get("usedIceWindPoints")
    sea_p = derived.get("usedSeaPressurePoints")
    sea_t = derived.get("usedSeaTempPoints")

    # Load quality = mostly is-data + freshness + trends
    load_score = 0.0
    load_score += 28.0 * norm(ice_p, 0.0, 3.0)
    load_score += 18.0 * norm(ice_t, 0.0, 3.0)
    load_score += 18.0 * norm(ice_w, 0.0, 3.0)
    load_score += 8.0 * norm(derived.get("usedIceWindDirPoints"), 0.0, 3.0)
    load_score += 10.0 * (1.0 if trend == "ok" else 0.55 if "partial" in trend else 0.25)
    load_score += 10.0 * (0.0 if stale else 1.0)
    load_score += 8.0 * (0.0 if "missing_now_fields" in quality_flags else 1.0)
    if "missing_ice_temperature_all" in quality_flags or "missing_ice_wind_all" in quality_flags:
        load_score -= 20.0
    if "missing_ice_temperature_partial" in quality_flags:
        load_score -= 8.0
    if "missing_ice_wind_partial" in quality_flags:
        load_score -= 8.0
    if "partial_point_fetch_errors" in quality_flags:
        load_score -= 5.0
    if "trend_gap_too_large" in quality_flags:
        load_score -= 10.0
    if "trend_gap12_too_large" in quality_flags:
        load_score -= 8.0
    load_score = clamp(load_score, 0.0, 100.0)

    # Hazard quality = broader field support + geometry + freshness
    hazard_score = 0.0
    hazard_score += 18.0 * norm(ice_p, 0.0, 3.0)
    hazard_score += 10.0 * norm(ice_t, 0.0, 3.0)
    hazard_score += 12.0 * norm(ice_w, 0.0, 3.0)
    hazard_score += 20.0 * norm(sea_p, 0.0, 8.0)
    hazard_score += 10.0 * norm(sea_t, 0.0, 10.0)
    hazard_score += 10.0 * (1.0 if trend == "ok" else 0.6 if "partial" in trend else 0.25)
    hazard_score += 10.0 * (0.0 if stale else 1.0)
    hazard_score += 10.0 * (0.0 if "missing_now_fields" in quality_flags else 1.0)
    if "partial_point_fetch_errors" in quality_flags:
        hazard_score -= 8.0
    if "missing_sea_temperature_partial" in quality_flags:
        hazard_score -= 4.0
    if "missing_gate_mid" in quality_flags or "missing_gate_east" in quality_flags or "missing_coast_gate" in quality_flags:
        hazard_score -= 6.0
    if "trend_gap_too_large" in quality_flags:
        hazard_score -= 12.0
    if "trend_gap12_too_large" in quality_flags:
        hazard_score -= 10.0
    hazard_score = clamp(hazard_score, 0.0, 100.0)

    load_level = quality_level(load_score)
    hazard_level = quality_level(hazard_score)
    data_status = "STALE" if stale else "FRESH"

    derived["loadQualityScore"] = int(round(load_score))
    derived["loadQualityLevel"] = load_level
    derived["loadQualityColor"] = quality_color(load_score)
    derived["hazardQualityScore"] = int(round(hazard_score))
    derived["hazardQualityLevel"] = hazard_level
    derived["hazardQualityColor"] = quality_color(hazard_score)
    derived["dataStatusLabel"] = data_status

    base_msg = output.get("message")
    if isinstance(base_msg, str) and base_msg.strip():
        base_main = base_msg.split(" LQ:")[0].split(" HQ:")[0].split(" D:")[0].strip()
        output["message"] = f"{base_main} {build_quality_tag(load_level, 'LQ')} {build_quality_tag(hazard_level, 'HQ')} D:{data_status}"

    output["phase"] = sanitize_phase_text(output.get("phase"))
    payload["derived"] = derived
    payload["output"] = output
    payload["meta"] = meta
    return payload


def write_summary_file(payload):
    meta = payload.get("meta", {})
    scores = payload.get("scores", {})
    inputs = payload.get("inputs", {})
    derived = payload.get("derived", {})
    output = payload.get("output", {})

    summary = {
        "updatedAt": meta.get("updatedAt"),
        "location": meta.get("location"),
        "model": meta.get("model"),
        "instanceId": meta.get("instanceId"),
        "lastSuccessfulUpdate": meta.get("lastSuccessfulUpdate"),
        "forecastInfo": meta.get("forecastInfo"),
        "stale": meta.get("stale", False),
        "level": output.get("level"),
        "phase": output.get("phase"),
        "risk": scores.get("risk"),
        "watch": derived.get("watch", False),
        "message": output.get("message"),
        "trendDataStatus": derived.get("trendDataStatus"),
        "trendGapHours6": derived.get("trendGapHours6"),
        "trendGapHours12": derived.get("trendGapHours12"),
        "qualityFlags": derived.get("qualityFlags", []),
        "reservoir": scores.get("reservoir"),
        "trigger": scores.get("trigger"),
        "coupling": scores.get("coupling"),
        "icePressure": inputs.get("icePressure"),
        "seaPressure": inputs.get("seaPressure"),
        "gradient": inputs.get("gradient"),
        "sf6": inputs.get("sf6"),
        "accG": derived.get("accG"),
        "regionalZone": derived.get("seaCentroidSector") or derived.get("sector"),
        "lpBearing": derived.get("lpBearingFromSea"),
        "lpBearingTrend6h": derived.get("lpBearingTrend6h"),
        "lpTrendLabel": derived.get("lpTrendLabel"),
        "lpDistanceToFavoredSector": derived.get("lpDistanceToFavoredSector"),
        "lpDistanceScore": derived.get("lpDistanceScore"),
        "lpGeometryScore": derived.get("lpGeometryScore"),
        "lpApproachSpeedDeg6h": derived.get("lpApproachSpeedDeg6h"),
        "lpSectorScore": derived.get("lpSectorScore"),
        "lpApproachingFavoredSector": derived.get("lpApproachingFavoredSector"),
        "sustainedPiteraqScore": derived.get("sustainedPiteraqScore"),
        "sustainedPiteraqStage": derived.get("sustainedPiteraqStage"),
        "sustainedPiteraqNearHits": derived.get("sustainedPiteraqNearHits"),
        "sustainedPiteraqStrongHits": derived.get("sustainedPiteraqStrongHits"),
        "katabaticLoadingScore": derived.get("katabaticLoadingScore"),
        "katabaticLoadingStage": derived.get("katabaticLoadingStage"),
        "katabaticColdHits24h": derived.get("katabaticColdHits24h"),
        "katabaticDeepColdHits24h": derived.get("katabaticDeepColdHits24h"),
        "katabaticReadinessLevel": derived.get("katabaticReadinessLevel"),
        "katabaticReadinessColor": derived.get("katabaticReadinessColor"),
        "piteraqHazardLevel": derived.get("piteraqHazardLevel"),
        "loadQualityScore": derived.get("loadQualityScore"),
        "loadQualityLevel": derived.get("loadQualityLevel"),
        "hazardQualityScore": derived.get("hazardQualityScore"),
        "hazardQualityLevel": derived.get("hazardQualityLevel"),
        "dataStatusLabel": derived.get("dataStatusLabel"),
    }

    save_json(SUMMARY_FILE, summary)


def get_json(url, params=None, retries=REQUEST_RETRIES):
    global _global_rate_limit_hits, _global_request_count
    last_err = None

    for attempt in range(retries):
        if _global_request_count >= _global_request_budget:
            raise RuntimeError(f"DMI request budget exhausted ({_global_request_budget})")
        if _global_rate_limit_hits >= _global_rate_limit_threshold:
            raise RuntimeError("DMI circuit breaker: rate limit threshold reached")

        try:
            time.sleep(REQUEST_SLEEP)
            _global_request_count += 1
            r = requests.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

            if r.status_code == 429:
                _global_rate_limit_hits += 1
                wait = 6 + attempt * 8
                print(f"DMI rate limit (429) hit #{_global_rate_limit_hits}. Waiting {wait}s...")
                time.sleep(wait)
                if _global_rate_limit_hits >= _global_rate_limit_threshold:
                    raise RuntimeError("DMI circuit breaker triggered")
                continue

            if r.status_code == 502:
                last_err = requests.exceptions.HTTPError("502 Bad Gateway")
                wait = 1 + attempt
                print(f"DMI request failed: HTTPError. Waiting {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except RuntimeError:
            raise

        except requests.exceptions.ReadTimeout as e:
            last_err = e
            wait = 2 + attempt * 2
            print(f"DMI read timeout. Waiting {wait}s before retry...")
            time.sleep(wait)

        except requests.exceptions.ConnectTimeout as e:
            last_err = e
            wait = 2 + attempt * 2
            print(f"DMI connect timeout. Waiting {wait}s before retry...")
            time.sleep(wait)

        except Exception as e:
            last_err = e
            wait = 1 + attempt * 2
            print(f"DMI request failed: {type(e).__name__}. Waiting {wait}s...")
            time.sleep(wait)

    raise last_err


def list_instances():
    url = f"{BASE}/collections/{COL}/instances"
    data = get_json(url)
    ids = []

    if isinstance(data, dict):
        if isinstance(data.get("instances"), list):
            for item in data["instances"]:
                if isinstance(item, dict):
                    iid = item.get("id") or item.get("instanceId")
                    if iid:
                        ids.append(iid)

        if not ids and isinstance(data.get("features"), list):
            for feat in data["features"]:
                iid = feat.get("id")
                if iid:
                    ids.append(iid)

        if not ids and isinstance(data.get("links"), list):
            for link in data["links"]:
                href = link.get("href", "")
                if "/instances/" in href:
                    iid = href.rstrip("/").split("/instances/")[-1].split("/")[0]
                    if iid:
                        ids.append(iid)

    ids = sorted(set(ids))
    if not ids:
        raise RuntimeError("Fant ingen DMI instanceId-er.")
    return ids


def fetch_position(instance_id, lon, lat, parameter_names, retries=REQUEST_RETRIES):
    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    params = {
        "coords": f"POINT({lon} {lat})",
        "parameter-name": ",".join(parameter_names),
        "crs": "crs84",
        "f": "CoverageJSON",
    }
    return get_json(url, params=params, retries=retries)


def parse_coverage_series(data, parameter_names):
    domain = data.get("domain", {})
    axes = domain.get("axes", {})
    times = axes.get("t", {}).get("values", [])
    ranges = data.get("ranges", {})
    values = {p: ranges.get(p, {}).get("values", []) for p in parameter_names}
    return times, values


def build_empty_cache():
    merged = {"ice": {}, "sea": {}}

    for p in ICE_POINTS:
        merged["ice"][p["name"]] = {
            "lon": p["lon"],
            "lat": p["lat"],
            "rows": [],
        }

    for p in SEA_POINTS + COAST_POINTS:
        merged["sea"][p["name"]] = {
            "lon": p["lon"],
            "lat": p["lat"],
            "rows": [],
        }

    return merged


def instance_is_reachable(instance_id):
    """Cheap health check: one point, one parameter, no retries."""
    global _global_request_count, _global_rate_limit_hits

    if _global_request_count >= _global_request_budget:
        print(f"Skipping health check for {instance_id}: request budget exhausted")
        return False
    if _global_rate_limit_hits >= _global_rate_limit_threshold:
        print(f"Skipping health check for {instance_id}: circuit breaker already active")
        return False

    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    params = {
        "coords": f"POINT({ICE_POINTS[0]['lon']} {ICE_POINTS[0]['lat']})",
        "parameter-name": "pressure-sealevel",
        "crs": "crs84",
        "f": "CoverageJSON",
    }

    try:
        time.sleep(min(REQUEST_SLEEP, 0.3))
        _global_request_count += 1
        r = requests.get(url, params=params, timeout=(INSTANCE_CHECK_CONNECT_TIMEOUT, INSTANCE_CHECK_READ_TIMEOUT))
        if r.status_code == 429:
            _global_rate_limit_hits += 1
            print(f"Instance health check hit 429 for {instance_id}")
            return False
        if r.status_code == 502:
            print(f"Instance {instance_id} health check got 502")
            return False
        if r.status_code != 200:
            print(f"Instance {instance_id} health check status {r.status_code}")
            return False
        return True
    except requests.exceptions.ReadTimeout:
        print(f"Instance {instance_id} health check timed out")
        return False
    except requests.exceptions.ConnectTimeout:
        print(f"Instance {instance_id} health check connect timed out")
        return False
    except Exception as e:
        print(f"Instance {instance_id} health check failed: {type(e).__name__}")
        return False


def choose_reachable_instance(candidates, label="instance"):
    """Return the first reachable instance from a candidate list, or None."""
    seen = set()
    cleaned = []
    for iid in candidates:
        if iid and iid not in seen:
            cleaned.append(iid)
            seen.add(iid)

    for iid in cleaned:
        if instance_is_reachable(iid):
            return iid
        print(f"Skipping {label} {iid}: instance health check failed")

    return None

def fetch_probe_position(instance_id, lon, lat, parameter_names):
    """Very cheap no-retry probe used only before extra fallback full fetch."""
    global _global_request_count, _global_rate_limit_hits

    if _global_request_count >= _global_request_budget:
        raise RuntimeError("probe request budget exhausted")
    if _global_rate_limit_hits >= _global_rate_limit_threshold:
        raise RuntimeError("probe blocked by circuit breaker")

    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    params = {
        "coords": f"POINT({lon} {lat})",
        "parameter-name": ",".join(parameter_names),
        "crs": "crs84",
        "f": "CoverageJSON",
    }

    time.sleep(min(EXTRA_PROBE_SLEEP, REQUEST_SLEEP))
    _global_request_count += 1
    r = requests.get(url, params=params, timeout=(EXTRA_PROBE_CONNECT_TIMEOUT, EXTRA_PROBE_READ_TIMEOUT))

    if r.status_code == 429:
        _global_rate_limit_hits += 1
        raise RuntimeError("probe_rate_limited")
    if r.status_code == 502:
        raise RuntimeError("probe_bad_gateway")

    r.raise_for_status()
    return r.json()


def instance_has_now_probe(instance_id, target_dt, tolerance_hours=TIME_TOLERANCE_HOURS, phase_label="now probe"):
    """Strict probe: allow full fetch only when both an ice point and a sea point expose near-now numeric fields."""
    if _global_rate_limit_hits > 0:
        print(f"Skipping {phase_label} for {instance_id}: rate limiting already observed")
        return False

    probe_specs = [
        ("ice", ICE_POINTS[0], ["pressure-sealevel", "temperature-2m", "wind-speed-100m", "wind-direction-100m"]),
        ("sea", SEA_POINTS_BACKFILL[1], ["pressure-sealevel", "temperature-2m"]),
    ]

    successes = 0
    for label, point, params in probe_specs:
        try:
            data = fetch_probe_position(instance_id, point["lon"], point["lat"], params)
            if not isinstance(data, dict):
                print(f"{phase_label.capitalize()} for {instance_id} returned no data at {label}:{point['name']}")
                return False

            times, values = parse_coverage_series(data, params)
            if not times:
                print(f"{phase_label.capitalize()} for {instance_id} found no valid times at {label}:{point['name']}")
                return False

            probe_axis = []
            for t in times:
                try:
                    probe_axis.append({"validTime": t, "dt": parse_iso(t)})
                except Exception:
                    continue

            valid_time, _, diff_h = find_valid_time(probe_axis, target_dt, tolerance_hours=tolerance_hours)
            if valid_time is None:
                print(f"{phase_label.capitalize()} for {instance_id} found no near-now timestep at {label}:{point['name']}")
                return False

            idx = times.index(valid_time)
            numeric_count = sum(1 for param in params if is_num(safe_get(values.get(param, []), idx)))
            min_required = 2 if label == "ice" else 1
            if numeric_count < min_required:
                print(f"{phase_label.capitalize()} for {instance_id} found too few numeric values at {label}:{point['name']} ({numeric_count})")
                return False

            print(f"{phase_label.capitalize()} ok for {instance_id} at {label}:{point['name']} ({valid_time}, diff {diff_h:.2f}h)")
            successes += 1
        except Exception as e:
            print(f"{phase_label.capitalize()} failed for {instance_id} at {label}:{point['name']}: {type(e).__name__}: {e}")
            return False

    return successes == len(probe_specs)


def fetch_points_parallel(instance_id, points, parameter_names, max_workers=MAX_WORKERS, retries=REQUEST_RETRIES):
    results = {}
    errors = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_position, instance_id, p["lon"], p["lat"], parameter_names, retries): p["name"]
            for p in points
        }

        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                if result is not None and not isinstance(result, dict):
                    raise RuntimeError(f"Unexpected fetch result type: {type(result).__name__}")
                results[name] = result
            except Exception as e:
                results[name] = None
                errors[name] = error_to_text(e)

    return results, errors


def append_instance_to_cache(cache, iid, sea_points=None, coast_points=None, request_retries=REQUEST_RETRIES):
    print(f"Fetching DMI series for instance {iid} with parallel point requests")

    sea_points = sea_points if sea_points is not None else SEA_POINTS
    coast_points = coast_points if coast_points is not None else COAST_POINTS

    ice_results, ice_errors = fetch_points_parallel(
        iid, ICE_POINTS, ICE_PARAMS, max_workers=min(MAX_WORKERS, len(ICE_POINTS)), retries=request_retries
    )
    sea_results, sea_errors = fetch_points_parallel(
        iid, sea_points + coast_points, SEA_PARAMS, max_workers=MAX_WORKERS, retries=request_retries
    )

    all_errors = {}
    all_errors.update({f"ice:{k}": v for k, v in ice_errors.items()})
    all_errors.update({f"sea:{k}": v for k, v in sea_errors.items()})

    for p in ICE_POINTS:
        data = ice_results.get(p["name"])
        if not isinstance(data, dict):
            continue
        try:
            times, values = parse_coverage_series(data, ICE_PARAMS)
            rows = cache["ice"][p["name"]]["rows"]

            for i, t in enumerate(times):
                rows.append(
                    {
                        "instanceId": iid,
                        "validTime": t,
                        "dt": parse_iso(t),
                        "pressure-sealevel": safe_get(values.get("pressure-sealevel", []), i),
                        "temperature-2m": safe_get(values.get("temperature-2m", []), i),
                        "wind-speed-100m": safe_get(values.get("wind-speed-100m", []), i),
                        "wind-direction-100m": safe_get(values.get("wind-direction-100m", []), i),
                    }
                )
        except Exception as e:
            all_errors[f"ice:{p['name']}"] = error_to_text(e)

    for p in sea_points + coast_points:
        data = sea_results.get(p["name"])
        if not isinstance(data, dict):
            continue
        try:
            times, values = parse_coverage_series(data, SEA_PARAMS)
            rows = cache["sea"][p["name"]]["rows"]

            for i, t in enumerate(times):
                rows.append(
                    {
                        "instanceId": iid,
                        "validTime": t,
                        "dt": parse_iso(t),
                        "pressure-sealevel": safe_get(values.get("pressure-sealevel", []), i),
                        "temperature-2m": safe_get(values.get("temperature-2m", []), i),
                    }
                )
        except Exception as e:
            all_errors[f"sea:{p['name']}"] = error_to_text(e)

    for block_type in ["ice", "sea"]:
        for block in cache[block_type].values():
            dedup = {}
            for row in block["rows"]:
                dedup[row["validTime"]] = row
            block["rows"] = sorted(dedup.values(), key=lambda x: x["dt"])

    return all_errors


def get_json_extra(url, params=None, retries=EXTRA_REQUEST_RETRIES):
    if _global_rate_limit_hits > 0:
        raise RuntimeError("Skipping extra fetch after rate limiting in core fetch")
    return get_json(url, params=params, retries=retries)


def fetch_position_extra(instance_id, lon, lat, parameter_names):
    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    params = {
        "coords": f"POINT({lon} {lat})",
        "parameter-name": ",".join(parameter_names),
        "crs": "crs84",
        "f": "CoverageJSON",
    }
    return get_json_extra(url, params=params)


def fetch_cloud_cover_bonus(instance_id, valid_time):
    cloud_vals = []
    errors = {}

    for p in ICE_POINTS:
        try:
            data = fetch_position_extra(instance_id, p["lon"], p["lat"], ICE_PARAMS_EXTRA)
            if not isinstance(data, dict):
                continue
            times, values = parse_coverage_series(data, ICE_PARAMS_EXTRA)
            if valid_time not in times:
                continue
            idx = times.index(valid_time)
            cc = safe_get(values.get("total-cloud-cover", []), idx)
            if is_num(cc):
                cloud_vals.append(cc)
        except Exception as e:
            errors[f"ice_extra:{p['name']}"] = f"{type(e).__name__}: {e}"

    cloud_cover_mean = avg(cloud_vals)
    radiative_cooling_proxy = (
        clamp(1.0 - cloud_cover_mean, 0.0, 1.0)
        if is_num(cloud_cover_mean)
        else None
    )

    return {
        "cloudCoverMean": cloud_cover_mean,
        "radiativeCoolingProxy": radiative_cooling_proxy,
        "usedIceCloudPoints": len(cloud_vals),
        "errors": errors,
    }


def build_master_time_axis(cache):
    dedup = {}
    for block_type in ["ice", "sea"]:
        for block in cache[block_type].values():
            for row in block["rows"]:
                dedup[row["validTime"]] = row["dt"]

    axis = [{"validTime": t, "dt": dt} for t, dt in dedup.items()]
    axis.sort(key=lambda x: x["dt"])
    return axis


def find_valid_time(master_axis, target_dt, tolerance_hours=TIME_TOLERANCE_HOURS):
    if not master_axis:
        return None, None, None

    best = None
    best_diff = None

    for item in master_axis:
        diff_h = abs((item["dt"] - target_dt).total_seconds()) / 3600.0
        if best is None or diff_h < best_diff:
            best = item
            best_diff = diff_h

    if best is None or best_diff is None or best_diff > tolerance_hours:
        return None, None, None

    return best["validTime"], best["dt"], best_diff


def find_best_valid_time_with_fields(cache, target_dt, tolerance_hours=TIME_TOLERANCE_HOURS):
    axis = build_master_time_axis(cache)
    if not axis:
        return None, None, None, None

    candidates = []
    for item in axis:
        diff_h = abs((item["dt"] - target_dt).total_seconds()) / 3600.0
        if diff_h <= tolerance_hours:
            candidates.append((diff_h, item))

    candidates.sort(key=lambda x: x[0])

    for diff_h, item in candidates:
        fields = fields_for_valid_time(cache, item["validTime"])
        if fields is not None:
            return item["validTime"], item["dt"], diff_h, fields

    return None, None, None, None


def get_row_for_valid_time(cache_block, point_name, valid_time):
    for row in cache_block[point_name]["rows"]:
        if row["validTime"] == valid_time:
            return row
    return None


def weighted_mean(vals, weights):
    pairs = [(v, w) for v, w in zip(vals, weights) if is_num(v)]
    if not pairs:
        return None
    sw = sum(w for _, w in pairs)
    if sw == 0:
        return None
    return sum(v * w for v, w in pairs) / sw


def circular_mean_deg(values):
    vals = [v for v in values if is_num(v)]
    if not vals:
        return None
    s = sum(math.sin(math.radians(v)) for v in vals)
    c = sum(math.cos(math.radians(v)) for v in vals)
    if abs(s) < 1e-12 and abs(c) < 1e-12:
        return None
    return normalize_angle(math.degrees(math.atan2(s, c)))


def direction_alignment_score(dir_deg, expected_bearing, max_bad=60.0):
    if not (is_num(dir_deg) and is_num(expected_bearing)):
        return None
    diff = abs(signed_shortest_angle_diff(dir_deg, expected_bearing))
    return clamp(1.0 - norm(diff, 0.0, max_bad), 0.0, 1.0)


def centroid_low(candidates):
    valid = [c for c in candidates if is_num(c.get("pressure"))]
    if not valid:
        return None, None, None, None

    pmax = max(c["pressure"] for c in valid)
    weights = [max(0.1, pmax - c["pressure"] + 0.1) for c in valid]
    sw = sum(weights)

    clon = sum(c["lon"] * w for c, w in zip(valid, weights)) / sw
    clat = sum(c["lat"] * w for c, w in zip(valid, weights)) / sw
    cp = sum(c["pressure"] * w for c, w in zip(valid, weights)) / sw

    nearest_name = None
    nearest_d2 = None
    for c in valid:
        d2 = (c["lon"] - clon) ** 2 + (c["lat"] - clat) ** 2
        if nearest_d2 is None or d2 < nearest_d2:
            nearest_d2 = d2
            nearest_name = c["name"]

    return cp, clon, clat, nearest_name


def mean_pressure_for_names(cache, valid_time, names):
    vals = []
    for name in names:
        row = get_row_for_valid_time(cache["sea"], name, valid_time)
        if not row:
            continue
        p = row.get("pressure-sealevel")
        if is_num(p):
            vals.append(p / 100.0)
    return avg(vals)


def cyclone_position_score(centroid_lon, centroid_lat, centroid_pressure):
    if not (is_num(centroid_lon) and is_num(centroid_lat)):
        return 0.0, False

    z = CYCLONE_TRIGGER_ZONE
    in_zone = (
        z["lat_min"] <= centroid_lat <= z["lat_max"]
        and z["lon_min"] <= centroid_lon <= z["lon_max"]
    )

    if not in_zone:
        return 0.0, False

    depth_score = norm(centroid_pressure, 1010.0, 990.0) if is_num(centroid_pressure) else 0.5
    return clamp(100.0 * depth_score, 0.0, 100.0), True


def is_ladning(reservoir, cyclone_in_zone, coupling):
    return reservoir >= 40 and (not cyclone_in_zone) and coupling < 45


def fields_for_valid_time(cache, valid_time):
    if valid_time is None:
        return None

    pressure_vals = []
    temp_vals = []
    wind_vals = []
    wind_dir_vals = []
    cloud_vals = []
    quality_flags = []

    for name in ["source", "mid", "mouth"]:
        row = get_row_for_valid_time(cache["ice"], name, valid_time)
        if not row:
            pressure_vals.append(None)
            temp_vals.append(None)
            wind_vals.append(None)
            wind_dir_vals.append(None)
            cloud_vals.append(None)
            continue

        p = row.get("pressure-sealevel")
        t = row.get("temperature-2m")
        w = row.get("wind-speed-100m")
        wd = row.get("wind-direction-100m")
        cc = row.get("total-cloud-cover")

        pressure_vals.append((p / 100.0) if is_num(p) else None)
        temp_vals.append(kelvin_to_celsius(t) if is_num(t) else None)
        wind_vals.append(w if is_num(w) else None)
        wind_dir_vals.append(wd if is_num(wd) else None)
        cloud_vals.append(cc if is_num(cc) else None)

    if sum(1 for v in pressure_vals if is_num(v)) == 0:
        return None

    ice_pressure = weighted_mean(pressure_vals, [0.40, 0.35, 0.25])
    ice_temp_c = weighted_mean(temp_vals, [0.45, 0.35, 0.20])
    ice_wind = weighted_mean(wind_vals, [0.20, 0.35, 0.45])

    ice_wind_dir_mean = circular_mean_deg(wind_dir_vals)
    dir_align_scores = [
        direction_alignment_score(d, EXPECTED_DRAINAGE_BEARING)
        for d in wind_dir_vals
        if is_num(d)
    ]
    katabatic_direction_alignment = avg(dir_align_scores)

    source_temp = temp_vals[0]
    mid_temp = temp_vals[1]
    mouth_temp = temp_vals[2]

    ice_stability_proxy = (
        source_temp - mouth_temp
        if is_num(source_temp) and is_num(mouth_temp)
        else None
    )
    ice_mid_stability_proxy = (
        mid_temp - mouth_temp
        if is_num(mid_temp) and is_num(mouth_temp)
        else None
    )

    cloud_cover_mean = avg(cloud_vals)
    radiative_cooling_proxy = (
        clamp(1.0 - cloud_cover_mean, 0.0, 1.0)
        if is_num(cloud_cover_mean)
        else None
    )

    if not is_num(ice_temp_c):
        quality_flags.append("missing_ice_temperature_all")
        ice_temp_c = None
    elif sum(1 for v in temp_vals if is_num(v)) < 3:
        quality_flags.append("missing_ice_temperature_partial")

    if not is_num(ice_wind):
        quality_flags.append("missing_ice_wind_all")
        ice_wind = None
    elif sum(1 for v in wind_vals if is_num(v)) < 3:
        quality_flags.append("missing_ice_wind_partial")

    if sum(1 for v in wind_dir_vals if is_num(v)) == 0:
        quality_flags.append("missing_ice_wind_direction_all")
    elif sum(1 for v in wind_dir_vals if is_num(v)) < 3:
        quality_flags.append("missing_ice_wind_direction_partial")

    if sum(1 for v in cloud_vals if is_num(v)) == 0:
        quality_flags.append("missing_cloud_cover_all")
    elif sum(1 for v in cloud_vals if is_num(v)) < 3:
        quality_flags.append("missing_cloud_cover_partial")

    if not is_num(ice_stability_proxy):
        quality_flags.append("missing_ice_stability_proxy")

    sea_candidates = []
    sea_temps = []

    for name in ALL_STRAIT_NAMES:
        row = get_row_for_valid_time(cache["sea"], name, valid_time)
        if not row:
            continue

        p = row.get("pressure-sealevel")
        t = row.get("temperature-2m")
        meta = cache["sea"][name]

        if is_num(p):
            sea_candidates.append(
                {
                    "name": name,
                    "pressure": p / 100.0,
                    "lon": meta["lon"],
                    "lat": meta["lat"],
                }
            )

        if is_num(t):
            sea_temps.append(kelvin_to_celsius(t))

    for name in ["K1", "K2"]:
        row = get_row_for_valid_time(cache["sea"], name, valid_time)
        if row and is_num(row.get("temperature-2m")):
            sea_temps.append(kelvin_to_celsius(row.get("temperature-2m")))

    if not sea_candidates:
        return None

    sea_min = min(sea_candidates, key=lambda x: x["pressure"])
    centroid_pressure, centroid_lon, centroid_lat, centroid_sector = centroid_low(sea_candidates)

    spread = None
    if is_num(centroid_pressure):
        spread = centroid_pressure - sea_min["pressure"]
        if spread >= 3.5:
            quality_flags.append("sea_field_spread_high")
        elif spread >= 2.0:
            quality_flags.append("sea_field_spread_moderate")

    sea_temp_c = avg(sea_temps)
    if sea_temp_c is None:
        quality_flags.append("missing_sea_temperature_all")
    elif len(sea_temps) < (len(ALL_STRAIT_NAMES) + 2):
        quality_flags.append("missing_sea_temperature_partial")

    west_mean = mean_pressure_for_names(cache, valid_time, WEST_NAMES)
    mid_mean = mean_pressure_for_names(cache, valid_time, MID_NAMES)
    east_mean = mean_pressure_for_names(cache, valid_time, EAST_NAMES)
    coast_mean = mean_pressure_for_names(cache, valid_time, ["K1", "K2"])

    gate_mid = None
    gate_east = None
    vent_index = None

    if is_num(west_mean) and is_num(mid_mean):
        gate_mid = west_mean - mid_mean
    if is_num(west_mean) and is_num(east_mean):
        gate_east = west_mean - east_mean
    if is_num(gate_mid) or is_num(gate_east):
        vent_index = (0.6 * (gate_mid if is_num(gate_mid) else 0.0)) + (0.4 * (gate_east if is_num(gate_east) else 0.0))

    if not is_num(gate_mid):
        quality_flags.append("missing_gate_mid")
    if not is_num(gate_east):
        quality_flags.append("missing_gate_east")

    coast_gate = None
    if is_num(ice_pressure) and is_num(coast_mean):
        coast_gate = ice_pressure - coast_mean
    else:
        quality_flags.append("missing_coast_gate")

    regional_ew_gradient = None
    regional_pressure_support = None
    if is_num(west_mean) and is_num(east_mean):
        regional_ew_gradient = east_mean - west_mean
        regional_pressure_support = 100.0 * norm(-regional_ew_gradient, -5.0, 5.0)

    return {
        "icePressure": ice_pressure,
        "iceTempC": ice_temp_c,
        "iceWind": ice_wind,
        "iceWindDirectionMean": ice_wind_dir_mean,
        "katabaticDirectionAlignment": katabatic_direction_alignment,
        "iceStabilityProxy": ice_stability_proxy,
        "iceMidStabilityProxy": ice_mid_stability_proxy,
        "cloudCoverMean": cloud_cover_mean,
        "radiativeCoolingProxy": radiative_cooling_proxy,
        "seaPressureMin": sea_min["pressure"],
        "seaMinSector": sea_min["name"],
        "seaMinLon": sea_min["lon"],
        "seaMinLat": sea_min["lat"],
        "seaCentroidPressure": centroid_pressure,
        "seaCentroidLon": centroid_lon,
        "seaCentroidLat": centroid_lat,
        "seaCentroidSector": centroid_sector,
        "seaMinCentroidSpread": spread,
        "seaTempC": sea_temp_c,
        "westMeanPressure": west_mean,
        "midMeanPressure": mid_mean,
        "eastMeanPressure": east_mean,
        "coastSeaPressure": coast_mean,
        "gateMid": gate_mid,
        "gateEast": gate_east,
        "ventilIndex": vent_index,
        "coastGate": coast_gate,
        "regionalEWGradient": regional_ew_gradient,
        "regionalPressureSupport": regional_pressure_support,
        "qualityFlags": sorted(set(quality_flags)),
        "usedIcePressurePoints": sum(1 for v in pressure_vals if is_num(v)),
        "usedIceTempPoints": sum(1 for v in temp_vals if is_num(v)),
        "usedIceWindPoints": sum(1 for v in wind_vals if is_num(v)),
        "usedIceWindDirPoints": sum(1 for v in wind_dir_vals if is_num(v)),
        "usedIceCloudPoints": sum(1 for v in cloud_vals if is_num(v)),
        "usedSeaPressurePoints": len(sea_candidates),
        "usedSeaTempPoints": len(sea_temps),
    }


def build_snapshot(snapshot_dt, fields):
    ice_pressure = fields["icePressure"]
    ice_temp_c = fields["iceTempC"]
    sea_pressure = fields["seaPressureMin"]
    gradient = (ice_pressure - sea_pressure) if is_num(ice_pressure) and is_num(sea_pressure) else None
    ice_wind = fields["iceWind"]
    ice_pressure_anom_now = (ice_pressure - ICE_PRESSURE_NORMAL_HPA) if is_num(ice_pressure) else None
    cold_support_now = max(0.0, -ice_temp_c) if is_num(ice_temp_c) else None
    dT_coast_ice = (
        max(0.0, fields["seaTempC"] - ice_temp_c)
        if is_num(fields["seaTempC"]) and is_num(ice_temp_c)
        else None
    )

    return {
        "t": iso_z(snapshot_dt),
        "icePressure": round(ice_pressure, 1) if is_num(ice_pressure) else None,
        "iceTempC": round(ice_temp_c, 1) if is_num(ice_temp_c) else None,
        "seaPressure": round(sea_pressure, 1) if is_num(sea_pressure) else None,
        "gradient": round(gradient, 1) if is_num(gradient) else None,
        "iceWind": round(ice_wind, 1) if is_num(ice_wind) else None,
        "iceWindDirectionMean": round(fields["iceWindDirectionMean"], 1) if is_num(fields.get("iceWindDirectionMean")) else None,
        "katabaticDirectionAlignment": round(fields["katabaticDirectionAlignment"], 3) if is_num(fields.get("katabaticDirectionAlignment")) else None,
        "iceStabilityProxy": round(fields["iceStabilityProxy"], 1) if is_num(fields.get("iceStabilityProxy")) else None,
        "radiativeCoolingProxy": round(fields["radiativeCoolingProxy"], 3) if is_num(fields.get("radiativeCoolingProxy")) else None,
        "regionalPressureSupport": round(fields["regionalPressureSupport"], 1) if is_num(fields.get("regionalPressureSupport")) else None,
        "ventilIndex": round(fields["ventilIndex"], 1) if is_num(fields["ventilIndex"]) else None,
        "coastGate": round(fields["coastGate"], 1) if is_num(fields["coastGate"]) else None,
        "seaMinLon": round(fields["seaMinLon"], 3) if is_num(fields["seaMinLon"]) else None,
        "seaMinLat": round(fields["seaMinLat"], 3) if is_num(fields["seaMinLat"]) else None,
        "seaCentroidPressure": round(fields["seaCentroidPressure"], 1) if is_num(fields["seaCentroidPressure"]) else None,
        "seaCentroidLon": round(fields["seaCentroidLon"], 3) if is_num(fields["seaCentroidLon"]) else None,
        "seaCentroidLat": round(fields["seaCentroidLat"], 3) if is_num(fields["seaCentroidLat"]) else None,
        "icePressureAnomNow": round(ice_pressure_anom_now, 1) if is_num(ice_pressure_anom_now) else None,
        "coldSupportNow": round(cold_support_now, 1) if is_num(cold_support_now) else None,
        "dTCoastIceNow": round(dT_coast_ice, 1) if is_num(dT_coast_ice) else None,
        "sector": fields["seaMinSector"],
    }


def sort_and_dedup_history(history):
    cleaned = []
    for item in history:
        try:
            dt = parse_iso(item["t"])
            cleaned.append((dt, item))
        except Exception:
            continue

    dedup = {}
    for dt, item in cleaned:
        dedup[item["t"]] = item

    out = sorted(dedup.values(), key=lambda x: parse_iso(x["t"]))
    return out[-HISTORY_KEEP:]


def find_history_snapshot(history, target_dt, tolerance, required_keys=None):
    required_keys = required_keys or []
    best = None
    best_diff = None

    for item in history:
        try:
            dt = parse_iso(item["t"])
        except Exception:
            continue

        if required_keys and not all(is_num(item.get(k)) for k in required_keys):
            continue

        diff = abs(dt - target_dt)
        if diff <= tolerance and (best_diff is None or diff < best_diff):
            best = item
            best_diff = diff

    return best


def missing_target_labels(history, now_dt):
    labels = []

    checks = [
        ("h6", now_dt - timedelta(hours=6), ["icePressure", "seaPressure", "iceWind", "ventilIndex", "coastGate"]),
        ("h12", now_dt - timedelta(hours=12), ["icePressure", "seaPressure", "ventilIndex", "coastGate"]),
    ]

    for label, target_dt, keys in checks:
        snap = find_history_snapshot(history, target_dt, TREND_TOLERANCES[label], required_keys=keys)
        if snap is None:
            labels.append(label)

    return labels


def backfill_until_targets_found(history, older_instance_ids, missing_labels):
    if not missing_labels or not older_instance_ids:
        return history, {}

    cache = build_empty_cache()
    fetch_errors = {}
    remaining = set(missing_labels)
    older_instance_ids = list(older_instance_ids)[-BACKFILL_MAX_INSTANCES:]

    for iid in reversed(older_instance_ids):
        if not instance_is_reachable(iid):
            print(f"Skipping {iid}: health check failed")
            fetch_errors[iid] = {"instance": "health_check_failed"}
            continue

        errs = append_instance_to_cache(
            cache,
            iid,
            sea_points=SEA_POINTS_BACKFILL,
            coast_points=[],
            request_retries=BACKFILL_REQUEST_RETRIES,
        )
        if errs:
            fetch_errors[iid] = errs

        axis = build_master_time_axis(cache)
        added_this_round = 0

        for label in list(remaining):
            hours_back = 6 if label == "h6" else 12
            now_snap = history[-1] if history else None
            if not now_snap:
                continue

            now_dt_hist = parse_iso(now_snap["t"])
            target_dt = now_dt_hist - timedelta(hours=hours_back)

            tolerance_hours = max(3.0, TREND_TOLERANCES[label].total_seconds() / 3600.0)
            valid_time, dt_found, diff_h = find_valid_time(axis, target_dt, tolerance_hours=tolerance_hours)
            if valid_time is None:
                continue

            fields = fields_for_valid_time(cache, valid_time)
            if not fields:
                continue

            snapshot = build_snapshot(dt_found, fields)
            history.append(snapshot)
            history = sort_and_dedup_history(history)
            remaining.discard(label)
            added_this_round += 1

        if added_this_round:
            print(f"Backfill added {added_this_round} snapshots after instance {iid}")

        if not remaining:
            print("Backfill stopping early: all missing targets filled")
            break

    return history, fetch_errors


def classify_risk(score):
    if score >= 85:
        return "RED", "PITERAQ WARNING", "0-6T"
    if score >= 65:
        return "ORG", "PITERAQ LIKELY", "6-12T"
    if score >= 45:
        return "YEL", "PITERAQ BUILDING", "12-24T"
    return "GRN", "PITERAQ LOW", "24-48T"


def classify_loading_level(score):
    if not is_num(score):
        return "LOW"
    elif score >= 75:
        return "CRITICAL"
    elif score >= 55:
        return "LOADED"
    elif score >= 35:
        return "BUILDING"
    else:
        return "LOW"


def classify_loading_color(score):
    if not is_num(score):
        return "GRN"
    elif score >= 75:
        return "RED"
    elif score >= 55:
        return "ORG"
    elif score >= 35:
        return "YEL"
    else:
        return "GRN"

def gradient_boost(gradient_hpa):
    return norm(gradient_hpa, 20, 40)


def potential_index(reservoir, coupling, gradient, d6, ice_wind_trend_6h):
    return 100 * (
        0.30 * (reservoir / 100.0)
        + 0.26 * (coupling / 100.0)
        + 0.24 * norm(gradient, 15, 40)
        + 0.10 * norm(d6 if is_num(d6) else 0.0, 0, 8)
        + 0.10 * norm(ice_wind_trend_6h if is_num(ice_wind_trend_6h) else 0.0, 0, 6)
    )

def sustained_piteraq_hit(row, strong=False):
    if not isinstance(row, dict):
        return False
    gradient = row.get("gradient")
    coast_gate = row.get("coastGate")
    ventil = row.get("ventilIndex")
    cold = row.get("coldSupportNow")
    if strong:
        return (
            is_num(gradient) and gradient >= 24.0
            and is_num(coast_gate) and coast_gate >= 18.0
            and is_num(ventil) and ventil >= 4.0
            and is_num(cold) and cold >= 36.0
        )
    return (
        is_num(gradient) and gradient >= 16.0
        and is_num(coast_gate) and coast_gate >= 12.0
        and is_num(ventil) and ventil >= 2.0
        and is_num(cold) and cold >= 30.0
    )


def recent_piteraq_rows(history, now_dt, hours=12):
    rows = []
    for item in history:
        if not isinstance(item, dict) or "t" not in item:
            continue
        try:
            dt = parse_iso(item["t"])
        except Exception:
            continue
        if dt <= now_dt and dt >= now_dt - timedelta(hours=hours):
            rows.append(item)
    rows.sort(key=lambda x: x["t"])
    return rows[-5:]


def sustained_piteraq_metrics(history, now_dt, current_snapshot):
    recent_rows = recent_piteraq_rows(history, now_dt, hours=12)
    near_hits = sum(1 for row in recent_rows if sustained_piteraq_hit(row, strong=False))
    strong_hits = sum(1 for row in recent_rows if sustained_piteraq_hit(row, strong=True))

    gradient_now = current_snapshot.get("gradient")
    coast_gate_now = current_snapshot.get("coastGate")
    ventil_now = current_snapshot.get("ventilIndex")
    cold_now = current_snapshot.get("coldSupportNow")

    cold_score = 100.0 * norm(cold_now, 32.0, 44.0)
    gradient_score = 100.0 * norm(gradient_now, 18.0, 30.0)
    coast_gate_score = 100.0 * norm(coast_gate_now, 16.0, 26.0)
    ventil_score = 100.0 * norm(ventil_now, 3.0, 8.0)

    current_near = sustained_piteraq_hit(current_snapshot, strong=False)
    current_strong = sustained_piteraq_hit(current_snapshot, strong=True)

    if current_strong and strong_hits >= 2:
        persistence_score = 100.0
    elif current_strong and near_hits >= 2:
        persistence_score = 92.0
    elif current_near and near_hits >= 2:
        persistence_score = 78.0
    elif current_near:
        persistence_score = 58.0
    else:
        persistence_score = 25.0 * near_hits

    score = (
        0.22 * cold_score
        + 0.22 * gradient_score
        + 0.22 * coast_gate_score
        + 0.14 * ventil_score
        + 0.20 * persistence_score
    )
    score = clamp(score, 0.0, 100.0)

    sustained_watch = current_near and near_hits >= 2
    sustained_likely = current_strong and near_hits >= 2
    sustained_warning = current_strong and (
        strong_hits >= 2
        or (
            is_num(gradient_now) and gradient_now >= 26.0
            and is_num(coast_gate_now) and coast_gate_now >= 22.0
            and is_num(cold_now) and cold_now >= 38.0
        )
    )

    stage = "none"
    if sustained_warning:
        stage = "warning"
    elif sustained_likely:
        stage = "likely"
    elif sustained_watch:
        stage = "watch"

    return {
        "score": score,
        "near_hits": near_hits,
        "strong_hits": strong_hits,
        "recent_rows": recent_rows,
        "current_near": current_near,
        "current_strong": current_strong,
        "watch": sustained_watch,
        "likely": sustained_likely,
        "warning": sustained_warning,
        "stage": stage,
    }


def katabatic_loading_metrics(
    history,
    now_dt,
    current_snapshot,
    dT_coast_ice_now,
    ice_temp_trend_24h,
    ice_temp_trend_72h,
    ice_pressure_anom_now,
    ice_anom_72_mean,
    ice_stability_proxy=None,
    katabatic_direction_alignment=None,
    radiative_cooling_proxy=None,
    regional_pressure_support=None,
):
    recent_rows = recent_piteraq_rows(history, now_dt, hours=24)
    cold_hits = sum(1 for row in recent_rows if is_num(row.get("coldSupportNow")) and row.get("coldSupportNow") >= 34.0)
    deep_cold_hits = sum(1 for row in recent_rows if is_num(row.get("coldSupportNow")) and row.get("coldSupportNow") >= 38.0)

    cold_now = current_snapshot.get("coldSupportNow")
    coast_gate_now = current_snapshot.get("coastGate")
    ventil_now = current_snapshot.get("ventilIndex")

    cold_now_score = 100.0 * norm(cold_now, 30.0, 46.0)
    pressure_memory_score = 100.0 * norm(ice_anom_72_mean, -12.0, 4.0) if is_num(ice_anom_72_mean) else 0.0
    dT_score = 100.0 * norm(dT_coast_ice_now, 28.0, 42.0)
    trend24_score = 100.0 * norm(-ice_temp_trend_24h, 0.0, 6.0) if is_num(ice_temp_trend_24h) else 0.0
    trend72_score = 100.0 * norm(-ice_temp_trend_72h, 0.0, 10.0) if is_num(ice_temp_trend_72h) else 0.0
    persistence_score = clamp(20.0 * cold_hits + 10.0 * deep_cold_hits, 0.0, 100.0)
    gate_support_score = 100.0 * norm(coast_gate_now, 8.0, 22.0) if is_num(coast_gate_now) else 0.0
    ventil_support_score = 100.0 * norm(ventil_now, 1.0, 5.0) if is_num(ventil_now) else 0.0
    pressure_now_score = 100.0 * norm(ice_pressure_anom_now, -12.0, 2.0) if is_num(ice_pressure_anom_now) else 0.0

    stability_score = 100.0 * norm(ice_stability_proxy, -5.0, 15.0) if is_num(ice_stability_proxy) else 0.0
    direction_score = 100.0 * katabatic_direction_alignment if is_num(katabatic_direction_alignment) else 50.0
    radiation_score = 100.0 * radiative_cooling_proxy if is_num(radiative_cooling_proxy) else 50.0
    regional_support_score = regional_pressure_support if is_num(regional_pressure_support) else 40.0

    score = (
        0.18 * cold_now_score
        + 0.10 * pressure_memory_score
        + 0.10 * dT_score
        + 0.08 * trend24_score
        + 0.06 * trend72_score
        + 0.14 * persistence_score
        + 0.06 * gate_support_score
        + 0.05 * ventil_support_score
        + 0.05 * pressure_now_score
        + 0.08 * stability_score
        + 0.10 * direction_score
        + 0.08 * radiation_score
        + 0.02 * regional_support_score
    )

    if is_num(cold_now):
        if cold_now >= 40.0:
            score += 10.0
        elif cold_now >= 38.0:
            score += 8.0
        elif cold_now >= 36.0:
            score += 6.0
        elif cold_now >= 34.0:
            score += 3.0

    if is_num(radiative_cooling_proxy):
        if radiative_cooling_proxy >= 0.8:
            score += 5.0
        elif radiative_cooling_proxy <= 0.25:
            score -= 3.0

    if is_num(katabatic_direction_alignment):
        if katabatic_direction_alignment >= 0.80:
            score += 6.0
        elif katabatic_direction_alignment <= 0.35:
            score -= 4.0

    score = clamp(score, 0.0, 100.0)

    loading = (
        is_num(cold_now) and cold_now >= 32.0
        and is_num(dT_coast_ice_now) and dT_coast_ice_now >= 28.0
        and (
            cold_hits >= 1
            or cold_now >= 35.0
            or stability_score >= 65.0
        )
    )
    primed = (
        is_num(cold_now) and cold_now >= 36.0
        and is_num(dT_coast_ice_now) and dT_coast_ice_now >= 34.0
        and cold_hits >= 2
        and (
            (is_num(coast_gate_now) and coast_gate_now >= 12.0)
            or (is_num(ventil_now) and ventil_now >= 2.0)
        )
        and (
            not is_num(katabatic_direction_alignment)
            or katabatic_direction_alignment >= 0.55
        )
    )

    stage = "none"
    if primed:
        stage = "primed"
    elif loading:
        stage = "loading"

    return {
        "score": score,
        "cold_hits": cold_hits,
        "deep_cold_hits": deep_cold_hits,
        "loading": loading,
        "primed": primed,
        "stage": stage,
        "stabilityScore": round(stability_score, 1),
        "directionScore": round(direction_score, 1),
        "radiationScore": round(radiation_score, 1),
        "regionalSupportScore": round(regional_support_score, 1),
    }


def build_payload(now_dt):
    reset_request_counters()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M UTC")
    history = sort_and_dedup_history(load_json(HISTORY_FILE, []))

    instance_ids = list_instances()
    latest = instance_ids[-1]
    prev_instance = instance_ids[-2] if len(instance_ids) >= 2 else None
    extra_now_instance = instance_ids[-3] if len(instance_ids) >= 3 else None
    primary_now_candidates = [latest] + ([prev_instance] if prev_instance else [])
    older_instance_ids = [iid for iid in instance_ids[:-1][-BACKFILL_MAX_INSTANCES:] if iid not in primary_now_candidates and iid != extra_now_instance]

    chosen_instance = None
    cache = None
    fetch_errors_now = {}
    valid_now = None
    dt_now = None
    diff_now = None
    now_fields = None

    ordered_primary_candidates = []
    seen_primary = set()
    for iid in primary_now_candidates:
        if iid and iid not in seen_primary:
            ordered_primary_candidates.append((iid, "primary now candidate"))
            seen_primary.add(iid)

    if older_instance_ids:
        for iid in reversed(older_instance_ids[-3:]):
            if iid and iid not in seen_primary:
                ordered_primary_candidates.append((iid, "older now fallback"))
                seen_primary.add(iid)

    for iid, label in ordered_primary_candidates:
        if not instance_is_reachable(iid):
            print(f"Skipping {label} {iid}: instance health check failed")
            continue
        if not instance_has_now_probe(iid, now_dt, tolerance_hours=TIME_TOLERANCE_HOURS, phase_label=label):
            print(f"Skipping {label} {iid}: strict now-field probe failed")
            continue

        trial_cache = build_empty_cache()
        trial_errors = append_instance_to_cache(trial_cache, iid)
        trial_valid_now, trial_dt_now, trial_diff_now, trial_now_fields = find_best_valid_time_with_fields(
            trial_cache, now_dt, tolerance_hours=TIME_TOLERANCE_HOURS
        )
        if trial_now_fields is not None:
            chosen_instance = iid
            cache = trial_cache
            fetch_errors_now = trial_errors
            valid_now = trial_valid_now
            dt_now = trial_dt_now
            diff_now = trial_diff_now
            now_fields = trial_now_fields
            break
        print(f"Skipping {label} {iid}: full fetch still lacked now-fields")

    if valid_now is None and extra_now_instance:
        print(f"Trying extra now fallback instance {extra_now_instance}")
        reset_request_counters(
            max_requests=FALLBACK_MAX_REQUESTS_PER_RUN,
            max_rate_limit_hits=FALLBACK_MAX_RATE_LIMIT_HITS,
        )
        selected_extra = choose_reachable_instance([extra_now_instance], label="extra now fallback")
        if selected_extra:
            if instance_has_now_probe(selected_extra, now_dt, tolerance_hours=9.0, phase_label="extra now fallback"):
                trial_cache = build_empty_cache()
                trial_errors = append_instance_to_cache(trial_cache, selected_extra, request_retries=BACKFILL_REQUEST_RETRIES)
                trial_valid_now, trial_dt_now, trial_diff_now, trial_now_fields = find_best_valid_time_with_fields(
                    trial_cache, now_dt, tolerance_hours=9.0
                )
                if trial_now_fields is not None:
                    chosen_instance = selected_extra
                    cache = trial_cache
                    fetch_errors_now = trial_errors
                    valid_now = trial_valid_now
                    dt_now = trial_dt_now
                    diff_now = trial_diff_now
                    now_fields = trial_now_fields
                else:
                    print(f"Skipping extra now fallback {selected_extra}: full fetch still lacked now-fields")
            else:
                print(f"Skipping extra now fallback {selected_extra}: strict now-field probe failed")

    if valid_now is None or now_fields is None:
        prev = load_json(DATA_FILE, {})
        if isinstance(prev, dict) and prev.get("inputs") and prev.get("derived"):
            prev_meta = prev.get("meta", {})
            prev_derived = prev.get("derived", {})
            prev_output = prev.get("output", {})

            qf = prev_derived.get("qualityFlags", [])
            if not isinstance(qf, list):
                qf = []

            prev_meta["updatedAt"] = now_utc_str()
            prev_meta["forecastInfo"] = "Now-felt manglet i DMI; beholdt siste gyldige datasett"
            prev_meta["lastAttemptFailed"] = "missing_now_fields"
            prev_meta["stale"] = True
            if not prev_meta.get("lastSuccessfulUpdate"):
                prev_meta["lastSuccessfulUpdate"] = prev_meta.get("updatedAt")

            prev_derived["trendDataStatus"] = "stale: missing_now_fields"
            prev_derived["qualityFlags"] = sorted(set(qf + ["missing_now_fields", "stale_due_to_failed_update"]))

            phase0 = sanitize_phase_text(prev_output.get("phase", "UNKNOWN"))
            if not phase0.startswith("STALE ("):
                prev_output["phase"] = sanitize_phase_text(f"STALE ({phase0})")
            else:
                prev_output["phase"] = phase0

            prev["meta"] = prev_meta
            prev["derived"] = prev_derived
            prev["output"] = prev_output
            return annotate_payload_quality(prev)

        raise RuntimeError("Kunne ikke lese now-felter fra DMI-data.")

    current_snapshot = build_snapshot(dt_now, now_fields)
    history.append(current_snapshot)
    history = sort_and_dedup_history(history)

    missing_labels = missing_target_labels(history, dt_now)
    fetch_errors_backfill = {}
    if missing_labels:
        print("Missing history targets:", missing_labels)
        history, fetch_errors_backfill = backfill_until_targets_found(history, older_instance_ids, missing_labels)

    save_json(HISTORY_FILE, history)

    fetch_errors = {}
    merged_now_errors = {}
    if fetch_errors_now:
        merged_now_errors.update(fetch_errors_now)
    if merged_now_errors:
        fetch_errors[chosen_instance or latest] = merged_now_errors
    fetch_errors.update(fetch_errors_backfill)

    snap_6 = find_history_snapshot(
        history, dt_now - timedelta(hours=6), TREND_TOLERANCES["h6"],
        required_keys=["icePressure", "seaPressure", "iceWind", "ventilIndex", "coastGate", "seaCentroidLon", "seaCentroidLat"]
    )
    snap_12 = find_history_snapshot(
        history, dt_now - timedelta(hours=12), TREND_TOLERANCES["h12"],
        required_keys=["icePressure", "seaPressure", "ventilIndex", "coastGate"]
    )
    snap_24 = find_history_snapshot(
        history, dt_now - timedelta(hours=24), TREND_TOLERANCES["h24"],
        required_keys=["iceTempC", "icePressure"]
    )
    snap_72 = find_history_snapshot(
        history, dt_now - timedelta(hours=72), TREND_TOLERANCES["h72"],
        required_keys=["iceTempC", "icePressure"]
    )

    quality_flags = sorted(set(now_fields.get("qualityFlags", [])))
    if fetch_errors:
        quality_flags.append("partial_point_fetch_errors")

    trend_gap_hours_6 = snapshot_gap_hours(current_snapshot, snap_6)
    trend_gap_hours_12 = snapshot_gap_hours(current_snapshot, snap_12)

    if snap_6 and is_num(trend_gap_hours_6) and not trend_gap_within_expected(trend_gap_hours_6, 6.0, 2.5):
        quality_flags.append("trend_gap_too_large")
        snap_6 = None

    if snap_12 and is_num(trend_gap_hours_12) and not trend_gap_within_expected(trend_gap_hours_12, 12.0, 3.5):
        quality_flags.append("trend_gap12_too_large")
        snap_12 = None

    count = sum(1 for x in [current_snapshot, snap_6, snap_12] if x is not None)
    if count == 3:
        trend_status = "ok"
    elif count == 2:
        trend_status = "partial"
    else:
        trend_status = "insufficient_distinct_steps"

    ice_pressure = current_snapshot["icePressure"]
    ice_temp_c = current_snapshot["iceTempC"]
    sea_pressure = current_snapshot["seaPressure"]
    gradient = current_snapshot["gradient"]
    ice_wind = current_snapshot["iceWind"]
    vent_now = current_snapshot["ventilIndex"]
    coast_gate_now = current_snapshot["coastGate"]
    sector = current_snapshot["sector"]

    d6 = (gradient - snap_6["gradient"]) if snap_6 and is_num(snap_6.get("gradient")) and is_num(gradient) else None
    d12 = (gradient - snap_12["gradient"]) if snap_12 and is_num(snap_12.get("gradient")) and is_num(gradient) else None

    sea_pressure_delta_6h = (
        sea_pressure - snap_6["seaPressure"]
        if snap_6 and is_num(snap_6.get("seaPressure")) and is_num(sea_pressure)
        else None
    )
    sea_pressure_delta_12h = (
        sea_pressure - snap_12["seaPressure"]
        if snap_12 and is_num(snap_12.get("seaPressure")) and is_num(sea_pressure)
        else None
    )

    sf6 = max(0.0, snap_6["seaPressure"] - sea_pressure) \
        if snap_6 and is_num(snap_6.get("seaPressure")) and is_num(sea_pressure) else None
    sf12 = max(0.0, snap_12["seaPressure"] - sea_pressure) \
        if snap_12 and is_num(snap_12.get("seaPressure")) and is_num(sea_pressure) else None

    earlier_6h_fall = None
    pressure_fall_acceleration = None
    if is_num(sf6) and is_num(sf12):
        earlier_6h_fall = max(0.0, sf12 - sf6)
        pressure_fall_acceleration = sf6 - earlier_6h_fall
        if pressure_fall_acceleration > 2.0:
            quality_flags.append("pressure_fall_accelerating")

    ice_wind_trend_6h = (ice_wind - snap_6["iceWind"]) if snap_6 and is_num(snap_6.get("iceWind")) and is_num(ice_wind) else None

    vent_d6 = (vent_now - snap_6["ventilIndex"]) if snap_6 and is_num(snap_6.get("ventilIndex")) and is_num(vent_now) else None
    vent_d12 = (vent_now - snap_12["ventilIndex"]) if snap_12 and is_num(snap_12.get("ventilIndex")) and is_num(vent_now) else None

    coast_gate_d6 = (coast_gate_now - snap_6["coastGate"]) if snap_6 and is_num(snap_6.get("coastGate")) and is_num(coast_gate_now) else None
    coast_gate_d12 = (coast_gate_now - snap_12["coastGate"]) if snap_12 and is_num(snap_12.get("coastGate")) and is_num(coast_gate_now) else None

    acc_uncertain = False
    if is_num(d6) and is_num(d12):
        acc_g = d6 - (d12 - d6)
    elif is_num(d6):
        acc_g = d6
        acc_uncertain = True
    else:
        acc_g = None
        acc_uncertain = True

    if is_num(sf6) and is_num(sf12):
        acc_s = pressure_fall_acceleration
    elif is_num(sf6):
        acc_s = sf6
        acc_uncertain = True
    else:
        acc_s = None
        acc_uncertain = True

    if trend_status == "partial":
        quality_flags.append("partial_trend_window")
    if acc_uncertain:
        quality_flags.append("acceleration_estimated")
    if not is_num(vent_now):
        quality_flags.append("missing_ventil_index")
    if not is_num(coast_gate_now):
        quality_flags.append("missing_coast_gate")

    sea_motion_km6 = None
    if snap_6 and is_num(now_fields["seaMinLon"]) and is_num(now_fields["seaMinLat"]):
        prev_lon = snap_6.get("seaMinLon")
        prev_lat = snap_6.get("seaMinLat")
        if is_num(prev_lon) and is_num(prev_lat):
            dx = (now_fields["seaMinLon"] - prev_lon) * math.cos(math.radians(now_fields["seaMinLat"])) * 111.0
            dy = (now_fields["seaMinLat"] - prev_lat) * 111.0
            sea_motion_km6 = math.sqrt(dx * dx + dy * dy)
            if sea_motion_km6 >= 180:
                quality_flags.append("sea_min_motion_high")

    ice_pressure_anom_now = current_snapshot["icePressureAnomNow"]
    dT_coast_ice = current_snapshot["dTCoastIceNow"]
    sea_low_depth = max(0.0, 1000.0 - sea_pressure) if is_num(sea_pressure) else None

    ice_temp_trend_24h = None
    ice_temp_trend_72h = None
    if snap_24 and is_num(ice_temp_c) and is_num(snap_24.get("iceTempC")):
        ice_temp_trend_24h = ice_temp_c - snap_24["iceTempC"]
    else:
        quality_flags.append("missing_ice_temp_trend_24h")

    if snap_72 and is_num(ice_temp_c) and is_num(snap_72.get("iceTempC")):
        ice_temp_trend_72h = ice_temp_c - snap_72["iceTempC"]
    else:
        quality_flags.append("missing_ice_temp_trend_72h")

    if is_num(ice_temp_trend_24h) and ice_temp_trend_24h <= -3.0:
        quality_flags.append("cold_reservoir_building_24h")
    if is_num(ice_temp_trend_72h) and ice_temp_trend_72h <= -5.0:
        quality_flags.append("cold_reservoir_building_72h")

    ice_pressure_trend_24h = (
        ice_pressure - snap_24["icePressure"]
        if snap_24 and is_num(snap_24.get("icePressure")) and is_num(ice_pressure)
        else None
    )
    ice_pressure_trend_72h = (
        ice_pressure - snap_72["icePressure"]
        if snap_72 and is_num(snap_72.get("icePressure")) and is_num(ice_pressure)
        else None
    )

    ice_anom_72 = [float(x.get("icePressureAnomNow", 0.0)) for x in history if is_num(x.get("icePressureAnomNow"))]
    cold_72 = [float(x.get("coldSupportNow", 0.0)) for x in history if is_num(x.get("coldSupportNow"))]
    dT_72 = [float(x.get("dTCoastIceNow", 0.0)) for x in history if is_num(x.get("dTCoastIceNow"))]

    ice_anom_72_mean = avg(ice_anom_72) if ice_anom_72 else 0.0
    cold_72_mean = avg(cold_72) if cold_72 else 0.0
    dT_72_mean = avg(dT_72) if dT_72 else 0.0

    reservoir = 100 * (
        0.50 * norm(ice_anom_72_mean, -12, 12)
        + 0.20 * norm(ice_pressure_anom_now, -12, 12)
        + 0.15 * norm(ice_pressure_trend_24h, -3, 8)
        + 0.10 * norm(ice_pressure_trend_72h, -5, 12)
        + 0.05 * (norm(-ice_temp_trend_24h, 0, 8) if is_num(ice_temp_trend_24h) else 0.0)
    )
    reservoir = clamp(reservoir, 0, 100)
    if is_num(ice_anom_72_mean) and ice_anom_72_mean <= -8:
        reservoir = min(reservoir, 39)

    sector_score = {
        "W1": 90, "W2": 92, "W3": 88,
        "C1": 78, "C2": 72,
        "M1": 80, "M2": 84, "M3": 82,
        "E1": 50, "E2": 35,
    }.get(sector, 25)

    cyclone_score, cyclone_in_zone = cyclone_position_score(
        current_snapshot.get("seaCentroidLon"),
        current_snapshot.get("seaCentroidLat"),
        current_snapshot.get("seaCentroidPressure"),
    )
    if not cyclone_in_zone:
        quality_flags.append("cyclone_outside_trigger_zone")

    sea_ref_lon = now_fields.get("seaMinLon")
    sea_ref_lat = now_fields.get("seaMinLat")
    lp_lon = now_fields.get("seaCentroidLon")
    lp_lat = now_fields.get("seaCentroidLat")
    lp_p = now_fields.get("seaCentroidPressure")

    lp_bearing_from_sea = bearing_deg(sea_ref_lon, sea_ref_lat, lp_lon, lp_lat)
    lp_sector_score_now = lp_sector_score(lp_bearing_from_sea)
    lp_alignment_now = lp_alignment_label(lp_bearing_from_sea)
    lp_distance_to_favored_deg = lp_distance_to_favored_sector(lp_bearing_from_sea)
    lp_distance_score_now = lp_distance_score(lp_distance_to_favored_deg)

    prev_lp_bearing = None
    lp_bearing_dist_now = None
    lp_bearing_trend_6h = None
    lp_approaching_favored = False
    lp_motion_cardinal_6h = "?"
    lp_motion_bearing = None
    lp_approach_speed_deg6h = None
    if snap_6:
        prev_lp_bearing = bearing_deg(
            snap_6.get("seaMinLon"),
            snap_6.get("seaMinLat"),
            snap_6.get("seaCentroidLon"),
            snap_6.get("seaCentroidLat"),
        )
        lp_trend = lp_trend_toward_favored_sector(lp_bearing_from_sea, prev_lp_bearing)
        lp_bearing_dist_now = lp_trend["center_distance_now"]
        lp_bearing_trend_6h = lp_trend["toward_deg"]
        lp_approaching_favored = lp_trend["approaching"]
        lp_approach_speed_deg6h = lp_bearing_trend_6h
        lp_motion_cardinal_6h, lp_motion_bearing = lp_motion_cardinal(
            snap_6.get("seaCentroidLon"),
            snap_6.get("seaCentroidLat"),
            lp_lon,
            lp_lat,
        )

    lp_trend_label = format_lpv_label(lp_bearing_trend_6h, lp_motion_cardinal_6h)
    lp_geometry_score_now = lp_geometry_score(
        lp_bearing_from_sea,
        lp_distance_to_favored_deg,
        lp_bearing_trend_6h,
        lp_approach_speed_deg6h,
    )
    lp_gate = lp_gate_factor(lp_distance_to_favored_deg, lp_bearing_trend_6h)

    lp_offset_hpa = (sea_pressure - lp_p) if is_num(sea_pressure) and is_num(lp_p) else None
    lp_distance_km = haversine_km(sea_ref_lon, sea_ref_lat, lp_lon, lp_lat)

    ice_stability_proxy = now_fields.get("iceStabilityProxy")
    katabatic_direction_alignment = now_fields.get("katabaticDirectionAlignment")
    radiative_cooling_proxy = now_fields.get("radiativeCoolingProxy")
    regional_pressure_support = now_fields.get("regionalPressureSupport")

    if lp_alignment_now == "core":
        quality_flags.append("lp_core_alignment")
    elif lp_alignment_now == "favored":
        quality_flags.append("lp_favored_alignment")
    if is_num(lp_distance_to_favored_deg) and lp_distance_to_favored_deg <= 5:
        quality_flags.append("lp_near_favored_sector")
    if lp_approaching_favored:
        quality_flags.append("lp_approaching_favored_sector")

    lp_geom_component = (
        0.70 * (lp_geometry_score_now / 100.0)
        + 0.30 * (lp_sector_score_now / 100.0)
    )

    direction_alignment_pct = (
        100.0 * katabatic_direction_alignment
        if is_num(katabatic_direction_alignment) else 50.0
    )

    coupling = 100 * (
        0.26 * (cyclone_score / 100.0)
        + 0.12 * lp_geom_component * lp_gate
        + 0.08 * (sector_score / 100.0)
        + 0.14 * norm(sea_low_depth, 5, 30)
        + 0.08 * norm(ice_wind, 4, 20)
        + 0.12 * norm(vent_now, 4, 16)
        + 0.08 * norm(coast_gate_now, 8, 30)
        + 0.12 * (direction_alignment_pct / 100.0)
    )
    if lp_alignment_now == "core" and lp_gate >= 0.85:
        coupling += 2.0
    elif lp_alignment_now == "favored" and lp_gate >= 0.70:
        coupling += 1.0
    if (
        is_num(katabatic_direction_alignment) and katabatic_direction_alignment >= 0.70
        and lp_alignment_now in {"core", "favored"}
    ):
        coupling += 4.0
    coupling = clamp(coupling, 0, 100)

    thermal_component = 100 * (
        0.45 * norm(dT_72_mean, 3, 20)
        + 0.25 * norm(cold_72_mean, 5, 25)
        + 0.15 * norm(dT_coast_ice, 10, 35)
        + 0.10 * (norm(-ice_temp_trend_24h, 0, 8) if is_num(ice_temp_trend_24h) else 0.0)
        + 0.05 * (norm(-ice_temp_trend_72h, 0, 12) if is_num(ice_temp_trend_72h) else 0.0)
    )
    thermal_component = clamp(thermal_component, 0, 100)

    reservoir_factor = 0.35 + 0.65 * (reservoir / 100.0)
    katabatic_potential = clamp(thermal_component * reservoir_factor, 0, 100)

    gboost = gradient_boost(gradient)
    pf_acc_score = norm(pressure_fall_acceleration, 0, 5) if is_num(pressure_fall_acceleration) else 0.0

    trigger = 100 * (
        0.23 * norm(gradient, 0, 65)
        + 0.08 * gboost
        + 0.18 * norm(sf6, 0, 10)
        + 0.07 * norm(sf12, 0, 14)
        + 0.06 * pf_acc_score
        + 0.10 * norm(d6, 0, 10)
        + 0.06 * norm(d12, 0, 14)
        + 0.05 * norm(acc_g, 0, 6)
        + 0.02 * norm(acc_s, 0, 6)
        + 0.05 * norm(ice_wind_trend_6h, 0, 6)
        + 0.05 * norm(vent_now, 4, 16)
        + 0.03 * norm(vent_d6, 0, 6)
        + 0.04 * norm(coast_gate_now, 8, 30)
        + 0.03 * norm(coast_gate_d6, 0, 8)
    )
    if lp_approaching_favored and is_num(lp_distance_to_favored_deg) and lp_distance_to_favored_deg <= 8:
        trigger += 0.8
    trigger = clamp(trigger, 0, 100)

    potential_raw = clamp(
        potential_index(reservoir, coupling, gradient, d6, ice_wind_trend_6h),
        0,
        100,
    )
    potential_reservoir_factor = 0.35 + 0.65 * (reservoir / 100.0)
    potential = clamp(potential_raw * potential_reservoir_factor, 0, 100)

    sustained = sustained_piteraq_metrics(history, dt_now, current_snapshot)
    sustained_score = sustained["score"]
    sustained_stage = sustained["stage"]
    sustained_near_hits = sustained["near_hits"]
    sustained_strong_hits = sustained["strong_hits"]

    loading = katabatic_loading_metrics(
        history,
        dt_now,
        current_snapshot,
        dT_coast_ice,
        ice_temp_trend_24h,
        ice_temp_trend_72h,
        ice_pressure_anom_now,
        ice_anom_72_mean,
        ice_stability_proxy=ice_stability_proxy,
        katabatic_direction_alignment=katabatic_direction_alignment,
        radiative_cooling_proxy=radiative_cooling_proxy,
        regional_pressure_support=regional_pressure_support,
    )
    katabatic_loading_score = loading["score"]
    katabatic_loading_stage = loading["stage"]
    katabatic_cold_hits = loading["cold_hits"]
    katabatic_deep_cold_hits = loading["deep_cold_hits"]
    loading_level = classify_loading_level(katabatic_loading_score)
    loading_color = classify_loading_color(katabatic_loading_score)

    watch = (
        (
            (reservoir >= 30 or potential >= 45)
            and coupling >= 60
            and (
                gradient >= 20
                or (is_num(d6) and d6 >= 2)
                or (is_num(sf6) and sf6 >= 2)
                or (is_num(pressure_fall_acceleration) and pressure_fall_acceleration >= 1.5)
                or (is_num(acc_g) and acc_g >= 1)
                or (is_num(ice_wind_trend_6h) and ice_wind_trend_6h >= 2)
                or (is_num(vent_now) and vent_now >= 8)
                or (is_num(coast_gate_now) and coast_gate_now >= 16)
                or (
                    lp_approaching_favored
                    and is_num(lp_distance_to_favored_deg)
                    and lp_distance_to_favored_deg <= 8
                    and coupling >= 55
                )
            )
        )
        or sustained["watch"]
        or loading["primed"]
    )

    base = 0.60 * trigger + 0.32 * reservoir + 0.08 * potential
    risk = base * (0.56 + 0.44 * (coupling / 100.0))
    risk = max(risk, 0.26 * katabatic_loading_score + 0.74 * risk)

    if katabatic_loading_score >= 40:
        risk = max(risk, 30.0)
    if katabatic_loading_score >= 55:
        risk = max(risk, 36.0)
    if katabatic_loading_score >= 70:
        risk = max(risk, 44.0)

    if loading["loading"]:
        risk = max(risk, 42.0)
    if loading["primed"]:
        risk = max(risk, 48.0)
    if sustained["watch"]:
        risk = max(risk, 55.0)
    if sustained["likely"]:
        risk = max(risk, 70.0)
    if sustained["warning"]:
        risk = max(risk, 82.0)

    piteraq_mismatch = (
        is_num(reservoir) and reservoir < 15
        and is_num(vent_now) and vent_now < 0
        and is_num(coast_gate_now) and coast_gate_now < 6
    )
    if piteraq_mismatch:
        risk *= 0.75
        potential *= 0.80
        trigger *= 0.90
        quality_flags.append("synoptic_storm_more_than_piteraq")

    if trigger < 20 and not loading["primed"] and not sustained["watch"]:
        risk = min(risk, 34)
    if trigger < 35 and reservoir < 25 and not loading["primed"] and not sustained["watch"]:
        risk = min(risk, 24)

    level, phase, horizon = classify_risk(risk)

    if sustained["warning"]:
        phase = "PITERAQ WARNING"
        horizon = "0-6T"
    elif sustained["likely"] and level in {"YEL", "ORG", "RED"}:
        phase = "PITERAQ SUSTAINED"
    elif sustained["watch"] and level in {"YEL", "ORG"}:
        phase = "PITERAQ WATCH"
    elif loading["primed"] and level in {"YEL", "ORG"}:
        phase = "KATABATIC PRIMED"
    elif loading["loading"] and level == "YEL":
        phase = "KATABATIC LOADING"
    elif katabatic_loading_score >= 40 and level == "YEL":
        phase = "KATABATIC BUILDING"

    if piteraq_mismatch and phase == "PITERAQ BUILDING":
        phase = "SYNOPTIC BUILDING"
    elif piteraq_mismatch and phase == "PITERAQ LIKELY":
        phase = "SYNOPTIC STORM"

    ladning_active = is_ladning(reservoir, cyclone_in_zone, coupling)
    if ladning_active and level == "GRN":
        phase = "LADNING"
    elif watch and level == "GRN":
        phase = "WATCH"

    if loading["loading"]:
        quality_flags.append("katabatic_loading")
    if loading["primed"]:
        quality_flags.append("katabatic_primed")
    if sustained["watch"]:
        quality_flags.append("sustained_piteraq_watch")
    if sustained["likely"]:
        quality_flags.append("sustained_piteraq_likely")
    if sustained["warning"]:
        quality_flags.append("sustained_piteraq_warning")

    trend_tag = "" if trend_status == "ok" else " T?"
    ag_tag = compact_score_tag("AG", acc_g, uncertain=acc_uncertain)
    as_tag = compact_score_tag("AS", acc_s, uncertain=acc_uncertain)
    ct24_tag = compact_temp_trend_tag("CT24", ice_temp_trend_24h)
    ct72_tag = compact_temp_trend_tag("CT72", ice_temp_trend_72h)
    vg_tag = compact_gate_tag("VG", vent_now)
    cg_tag = compact_gate_tag("CG", coast_gate_now)
    lpb_tag = compact_score_tag("LPB", lp_bearing_from_sea)
    lpv_tag = lp_trend_label
    lpd_tag = compact_score_tag("LPD", lp_distance_to_favored_deg)
    lpg_tag = compact_score_tag("LPG", lp_geometry_score_now)
    spt_tag = compact_score_tag("SPT", sustained_score)
    lad_tag = " LAD" if ladning_active else ""

    message = (
        f"{level}{int(round(risk))} "
        f"LOAD{loading_level} PIT{level} "
        f"H{horizon.strip()} "
        f"RES{int(round(reservoir))} TRG{int(round(trigger))} CPL{int(round(coupling))} "
        f"GR{gradient:.1f} "
        f"{lpv_tag} {lpd_tag} "
        f"VG{int(round(vent_now)) if is_num(vent_now) else '?'} "
        f"CG{int(round(coast_gate_now)) if is_num(coast_gate_now) else '?'}"
    )

    payload = {
        "meta": {
            "source": "DMI HARMONIE",
            "updatedAt": now_str,
            "location": LOCATION_NAME,
            "model": COL,
            "forecastInfo": "Latest available forecast step",
            "instanceId": chosen_instance or latest,
            "lastSuccessfulUpdate": now_str,
            "lastAttemptFailed": None,
            "stale": False,
        },
        "inputs": {
            "icePressure": round(ice_pressure, 1) if is_num(ice_pressure) else None,
            "seaPressure": round(sea_pressure, 1) if is_num(sea_pressure) else None,
            "gradient": round(gradient, 1) if is_num(gradient) else None,
            "d6": round(d6, 1) if is_num(d6) else None,
            "d12": round(d12, 1) if is_num(d12) else None,
            "seaPressureDelta6h": round(sea_pressure_delta_6h, 1) if is_num(sea_pressure_delta_6h) else None,
            "seaPressureDelta12h": round(sea_pressure_delta_12h, 1) if is_num(sea_pressure_delta_12h) else None,
            "sf6": round(sf6, 1) if is_num(sf6) else None,
            "sf12": round(sf12, 1) if is_num(sf12) else None,
            "pressureFallAcceleration": round(pressure_fall_acceleration, 1) if is_num(pressure_fall_acceleration) else None,
            "iceWind": round(ice_wind, 1) if is_num(ice_wind) else None,
            "iceWindDirectionMean": round(now_fields["iceWindDirectionMean"], 1) if is_num(now_fields.get("iceWindDirectionMean")) else None,
            "iceWindTrend6h": round(ice_wind_trend_6h, 1) if is_num(ice_wind_trend_6h) else None,
            "coastIceDeltaT": round(dT_coast_ice, 1) if is_num(dT_coast_ice) else None,
            "iceTempTrend24h": round(ice_temp_trend_24h, 1) if is_num(ice_temp_trend_24h) else None,
            "iceTempTrend72h": round(ice_temp_trend_72h, 1) if is_num(ice_temp_trend_72h) else None,
            "ventilIndex": round(vent_now, 1) if is_num(vent_now) else None,
            "ventilD6": round(vent_d6, 1) if is_num(vent_d6) else None,
            "ventilD12": round(vent_d12, 1) if is_num(vent_d12) else None,
            "coastSeaPressure": round(now_fields["coastSeaPressure"], 1) if is_num(now_fields["coastSeaPressure"]) else None,
            "coastGate": round(coast_gate_now, 1) if is_num(coast_gate_now) else None,
            "coastGateD6": round(coast_gate_d6, 1) if is_num(coast_gate_d6) else None,
            "coastGateD12": round(coast_gate_d12, 1) if is_num(coast_gate_d12) else None,
        },
        "scores": {
            "reservoir": int(round(reservoir)),
            "trigger": int(round(trigger)),
            "coupling": int(round(coupling)),
            "potential": int(round(potential)),
            "risk": int(round(risk)),
        },
        "derived": {
            "watch": watch,
            "isLadning": ladning_active,
            "sector": sector,
            "cycloneInTriggerZone": cyclone_in_zone,
            "cycloneScore": int(round(cyclone_score)),
            "piteraqMismatch": piteraq_mismatch,
            "usedInstanceIds": [chosen_instance] if chosen_instance else [],
            "fetchErrors": sanitize_fetch_errors(fetch_errors),
            "seaMinLon": round(now_fields["seaMinLon"], 3) if is_num(now_fields["seaMinLon"]) else None,
            "seaMinLat": round(now_fields["seaMinLat"], 3) if is_num(now_fields["seaMinLat"]) else None,
            "seaCentroidPressure": round(now_fields["seaCentroidPressure"], 1) if is_num(now_fields["seaCentroidPressure"]) else None,
            "seaCentroidLon": round(now_fields["seaCentroidLon"], 3) if is_num(now_fields["seaCentroidLon"]) else None,
            "seaCentroidLat": round(now_fields["seaCentroidLat"], 3) if is_num(now_fields["seaCentroidLat"]) else None,
            "seaCentroidSector": now_fields["seaCentroidSector"],
            "seaMinCentroidSpread": round(now_fields["seaMinCentroidSpread"], 1) if is_num(now_fields["seaMinCentroidSpread"]) else None,
            "seaMinMotionKm6": round(sea_motion_km6, 1) if is_num(sea_motion_km6) else None,
            "westMeanPressure": round(now_fields["westMeanPressure"], 1) if is_num(now_fields["westMeanPressure"]) else None,
            "midMeanPressure": round(now_fields["midMeanPressure"], 1) if is_num(now_fields["midMeanPressure"]) else None,
            "eastMeanPressure": round(now_fields["eastMeanPressure"], 1) if is_num(now_fields["eastMeanPressure"]) else None,
            "coastSeaPressure": round(now_fields["coastSeaPressure"], 1) if is_num(now_fields["coastSeaPressure"]) else None,
            "gateMid": round(now_fields["gateMid"], 1) if is_num(now_fields["gateMid"]) else None,
            "gateEast": round(now_fields["gateEast"], 1) if is_num(now_fields["gateEast"]) else None,
            "ventilIndex": round(vent_now, 1) if is_num(vent_now) else None,
            "ventilD6": round(vent_d6, 1) if is_num(vent_d6) else None,
            "ventilD12": round(vent_d12, 1) if is_num(vent_d12) else None,
            "coastGate": round(coast_gate_now, 1) if is_num(coast_gate_now) else None,
            "coastGateD6": round(coast_gate_d6, 1) if is_num(coast_gate_d6) else None,
            "coastGateD12": round(coast_gate_d12, 1) if is_num(coast_gate_d12) else None,
            "icePressureAnomNow": round(ice_pressure_anom_now, 1) if is_num(ice_pressure_anom_now) else None,
            "iceStabilityProxy": round(ice_stability_proxy, 1) if is_num(ice_stability_proxy) else None,
            "katabaticDirectionAlignment": round(katabatic_direction_alignment, 3) if is_num(katabatic_direction_alignment) else None,
            "iceWindDirectionMean": round(now_fields["iceWindDirectionMean"], 1) if is_num(now_fields.get("iceWindDirectionMean")) else None,
            "cloudCoverMean": round(now_fields["cloudCoverMean"], 3) if is_num(now_fields.get("cloudCoverMean")) else None,
            "radiativeCoolingProxy": round(radiative_cooling_proxy, 3) if is_num(radiative_cooling_proxy) else None,
            "regionalEWGradient": round(now_fields["regionalEWGradient"], 2) if is_num(now_fields.get("regionalEWGradient")) else None,
            "regionalPressureSupport": round(regional_pressure_support, 1) if is_num(regional_pressure_support) else None,
            "icePressureAnom72hMean": round(ice_anom_72_mean, 1) if is_num(ice_anom_72_mean) else None,
            "icePressureTrend24h": round(ice_pressure_trend_24h, 1) if is_num(ice_pressure_trend_24h) else None,
            "icePressureTrend72h": round(ice_pressure_trend_72h, 1) if is_num(ice_pressure_trend_72h) else None,
            "coldSupport72h": round(cold_72_mean, 1) if is_num(cold_72_mean) else None,
            "katabaticPotential": int(round(katabatic_potential)),
            "seaLowDepth": round(sea_low_depth, 1) if is_num(sea_low_depth) else None,
            "gradientBoost": round(gboost, 2),
            "accG": round(acc_g, 1) if is_num(acc_g) else None,
            "accS": round(acc_s, 1) if is_num(acc_s) else None,
            "earlier6hFall": round(earlier_6h_fall, 1) if is_num(earlier_6h_fall) else None,
            "pressureFallAcceleration": round(pressure_fall_acceleration, 1) if is_num(pressure_fall_acceleration) else None,
            "potentialRaw": int(round(potential_raw)),
            "potentialReservoirFactor": round(potential_reservoir_factor, 2),
            "lpBearingFromSea": round(lp_bearing_from_sea, 1) if is_num(lp_bearing_from_sea) else None,
            "lpPreviousBearingFromSea": round(prev_lp_bearing, 1) if is_num(prev_lp_bearing) else None,
            "lpBearingDistanceToNorth": round(lp_bearing_dist_now, 1) if is_num(lp_bearing_dist_now) else None,
            "lpBearingTrend6h": round(lp_bearing_trend_6h, 1) if is_num(lp_bearing_trend_6h) else None,
            "lpTrendLabel": lp_trend_label,
            "lpMotionBearing6h": round(lp_motion_bearing, 1) if is_num(lp_motion_bearing) else None,
            "lpMotionCardinal6h": lp_motion_cardinal_6h,
            "lpApproachSpeedDeg6h": round(lp_approach_speed_deg6h, 1) if is_num(lp_approach_speed_deg6h) else None,
            "lpApproachingFavoredSector": lp_approaching_favored,
            "lpDistanceToFavoredSector": round(lp_distance_to_favored_deg, 1) if is_num(lp_distance_to_favored_deg) else None,
            "lpDistanceScore": int(round(lp_distance_score_now)),
            "lpSectorScore": int(round(lp_sector_score_now)),
            "lpGeometryScore": int(round(lp_geometry_score_now)),
            "lpGeometryGate": round(lp_gate, 2),
            "lpAlignmentLabel": lp_alignment_now,
            "sustainedPiteraqScore": int(round(sustained_score)),
            "sustainedPiteraqStage": sustained_stage,
            "sustainedPiteraqNearHits": sustained_near_hits,
            "sustainedPiteraqStrongHits": sustained_strong_hits,
            "katabaticLoadingScore": int(round(katabatic_loading_score)),
            "katabaticLoadingStage": katabatic_loading_stage,
            "katabaticStabilityScore": loading.get("stabilityScore"),
            "katabaticDirectionScore": loading.get("directionScore"),
            "katabaticRadiationScore": loading.get("radiationScore"),
            "katabaticRegionalSupportScore": loading.get("regionalSupportScore"),
            "katabaticColdHits24h": katabatic_cold_hits,
            "katabaticDeepColdHits24h": katabatic_deep_cold_hits,
            "katabaticReadinessLevel": loading_level,
            "katabaticReadinessColor": loading_color,
            "piteraqHazardLevel": level,            
            "lpOffsetHpa": round(lp_offset_hpa, 1) if is_num(lp_offset_hpa) else None,
            "lpDistanceKm": round(lp_distance_km, 1) if is_num(lp_distance_km) else None,
            "accUncertain": acc_uncertain,
            "trendDataStatus": trend_status,
            "trendGapHours6": round(trend_gap_hours_6, 2) if is_num(trend_gap_hours_6) else None,
            "trendGapHours12": round(trend_gap_hours_12, 2) if is_num(trend_gap_hours_12) else None,
            "qualityFlags": sorted(set(quality_flags)),
            "selectedTimes": {
                "now": {"validTime": valid_now, "diffHours": round(diff_now, 2)} if valid_now else None,
                "h6_from_history": snap_6["t"] if snap_6 else None,
                "h12_from_history": snap_12["t"] if snap_12 else None,
                "h24_from_history": snap_24["t"] if snap_24 else None,
                "h72_from_history": snap_72["t"] if snap_72 else None,
            },
            "usedIcePressurePoints": now_fields["usedIcePressurePoints"],
            "usedIceTempPoints": now_fields["usedIceTempPoints"],
            "usedIceWindPoints": now_fields["usedIceWindPoints"],
            "usedIceWindDirPoints": now_fields["usedIceWindDirPoints"],
            "usedIceCloudPoints": now_fields["usedIceCloudPoints"],
            "usedSeaPressurePoints": now_fields["usedSeaPressurePoints"],
            "usedSeaTempPoints": now_fields["usedSeaTempPoints"],
        },
        "output": {
            "level": level,
            "phase": phase,
            "message": message,
        },
    }

    return payload


def write_stale_payload(error):
    existing = load_json(DATA_FILE, None)
    err_name = type(error).__name__
    err_text = f"{err_name}"

    if isinstance(existing, dict) and existing.get("inputs") and existing.get("scores") and existing.get("derived"):
        meta = existing.get("meta", {})
        derived = existing.get("derived", {})
        quality_flags = derived.get("qualityFlags", [])
        if not isinstance(quality_flags, list):
            quality_flags = []

        quality_flags = sorted(set(quality_flags + ["stale_due_to_failed_update"]))

        meta["updatedAt"] = now_utc_str()
        meta["source"] = "DMI HARMONIE"
        meta["location"] = LOCATION_NAME
        meta["model"] = COL
        meta["forecastInfo"] = f"Latest update failed: {err_name}"
        meta["lastAttemptFailed"] = err_text
        meta["stale"] = True
        meta.setdefault("lastSuccessfulUpdate", meta.get("updatedAt"))

        derived["trendDataStatus"] = f"stale: {err_name}"
        derived["qualityFlags"] = quality_flags
        derived["fetchErrors"] = sanitize_fetch_errors(derived.get("fetchErrors", {}))

        existing["meta"] = meta
        existing["derived"] = derived

        existing_output = existing.get("output", {})
        phase0 = sanitize_phase_text(existing_output.get("phase", "UNKNOWN"))
        if not phase0.startswith("STALE ("):
            existing_output["phase"] = sanitize_phase_text(f"STALE ({phase0})")
        else:
            existing_output["phase"] = phase0
        existing["output"] = existing_output
        annotate_payload_quality(existing)

        save_json(DATA_FILE, existing)
        write_summary_file(existing)
        print(f"Preserved last good dataset; marked as stale due to {err_name}")
        return

    fallback = {
        "meta": {
            "source": "DMI HARMONIE",
            "updatedAt": now_utc_str(),
            "location": LOCATION_NAME,
            "model": COL,
            "forecastInfo": f"Update failed: {err_name}",
            "instanceId": "-",
            "lastSuccessfulUpdate": None,
            "lastAttemptFailed": err_text,
            "stale": True,
        },
        "inputs": {
            "icePressure": None,
            "seaPressure": None,
            "gradient": None,
            "d6": None,
            "d12": None,
            "seaPressureDelta6h": None,
            "seaPressureDelta12h": None,
            "sf6": None,
            "sf12": None,
            "pressureFallAcceleration": None,
            "iceWind": None,
            "iceWindDirectionMean": None,
            "iceWindTrend6h": None,
            "coastIceDeltaT": None,
            "iceTempTrend24h": None,
            "iceTempTrend72h": None,
            "ventilIndex": None,
            "ventilD6": None,
            "ventilD12": None,
            "coastSeaPressure": None,
            "coastGate": None,
            "coastGateD6": None,
            "coastGateD12": None,
        },
        "scores": {
            "reservoir": None,
            "trigger": None,
            "coupling": None,
            "potential": None,
            "risk": None,
        },
        "derived": {
            "watch": False,
            "isLadning": False,
            "sector": None,
            "cycloneInTriggerZone": False,
            "cycloneScore": 0,
            "piteraqMismatch": False,
            "usedInstanceIds": [],
            "fetchErrors": {},
            "seaMinLon": None,
            "seaMinLat": None,
            "seaCentroidPressure": None,
            "seaCentroidLon": None,
            "seaCentroidLat": None,
            "seaCentroidSector": None,
            "seaMinCentroidSpread": None,
            "seaMinMotionKm6": None,
            "westMeanPressure": None,
            "midMeanPressure": None,
            "eastMeanPressure": None,
            "coastSeaPressure": None,
            "gateMid": None,
            "gateEast": None,
            "ventilIndex": None,
            "ventilD6": None,
            "ventilD12": None,
            "coastGate": None,
            "coastGateD6": None,
            "coastGateD12": None,
            "icePressureAnomNow": None,
            "iceStabilityProxy": None,
            "katabaticDirectionAlignment": None,
            "iceWindDirectionMean": None,
            "cloudCoverMean": None,
            "radiativeCoolingProxy": None,
            "regionalEWGradient": None,
            "regionalPressureSupport": None,
            "icePressureAnom72hMean": None,
            "icePressureTrend24h": None,
            "icePressureTrend72h": None,
            "coldSupport72h": None,
            "katabaticPotential": None,
            "seaLowDepth": None,
            "gradientBoost": None,
            "accG": None,
            "accS": None,
            "earlier6hFall": None,
            "pressureFallAcceleration": None,
            "potentialRaw": None,
            "potentialReservoirFactor": None,
            "lpBearingFromSea": None,
            "lpPreviousBearingFromSea": None,
            "lpBearingDistanceToNorth": None,
            "lpBearingTrend6h": None,
            "lpTrendLabel": None,
            "lpMotionBearing6h": None,
            "lpMotionCardinal6h": None,
            "lpApproachSpeedDeg6h": None,
            "lpApproachingFavoredSector": False,
            "lpDistanceToFavoredSector": None,
            "lpDistanceScore": None,
            "lpSectorScore": None,
            "lpGeometryScore": None,
            "lpGeometryGate": None,
            "lpAlignmentLabel": None,
            "sustainedPiteraqScore": None,
            "sustainedPiteraqStage": None,
            "sustainedPiteraqNearHits": None,
            "sustainedPiteraqStrongHits": None,
            "katabaticLoadingScore": None,
            "katabaticLoadingStage": None,
            "katabaticStabilityScore": None,
            "katabaticDirectionScore": None,
            "katabaticRadiationScore": None,
            "katabaticRegionalSupportScore": None,
            "katabaticColdHits24h": None,
            "katabaticDeepColdHits24h": None,
            "lpOffsetHpa": None,
            "lpDistanceKm": None,
            "accUncertain": False,
            "trendDataStatus": f"error: {err_name}",
            "trendGapHours6": None,
            "trendGapHours12": None,
            "qualityFlags": ["hard_failure"],
            "selectedTimes": {
                "now": None,
                "h6_from_history": None,
                "h12_from_history": None,
                "h24_from_history": None,
                "h72_from_history": None,
            },
            "usedIcePressurePoints": 0,
            "usedIceTempPoints": 0,
            "usedIceWindPoints": 0,
            "usedIceWindDirPoints": 0,
            "usedIceCloudPoints": 0,
            "usedSeaPressurePoints": 0,
            "usedSeaTempPoints": 0,
        },
        "output": {
            "level": "GRN",
            "phase": "ERROR",
            "message": f"{LOCATION_NAME} ERROR DMI {err_name}",
        },
    }
    annotate_payload_quality(fallback)
    save_json(DATA_FILE, fallback)
    write_summary_file(fallback)
    print(f"No prior good dataset; wrote fallback due to {err_name}")


if __name__ == "__main__":
    try:
        payload = annotate_payload_quality(build_payload(now_utc()))
        save_json(DATA_FILE, payload)
        write_summary_file(payload)
        print("Updated data.json/history.json/summary.json successfully")
        print("trendDataStatus:", payload["derived"]["trendDataStatus"])
        print("used instances:", payload["derived"]["usedInstanceIds"])
        if payload["derived"].get("qualityFlags"):
            print("qualityFlags:", payload["derived"]["qualityFlags"])
    except Exception as e:
        print("Update failed:", repr(e))
        write_stale_payload(e)

