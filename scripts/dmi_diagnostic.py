"""
DMI HARMONIE IG API Diagnostic Script
======================================
Skrivebeskyttet — berører IKKE data.json, history.json eller summary.json.
Kjøres manuelt for å verifisere at DMI-APIet svarer korrekt på våre kall.

Kjøring:
    python dmi_diagnostic.py

Krav:
    pip install requests
"""

import sys
import time
import requests

BASE = "https://opendataapi.dmi.dk/v1/forecastedr"
COL = "harmonie_ig_sf"
TIMEOUT = (8, 20)

# Alle parameterne vi bruker i produksjon
PARAMS_TO_TEST = [
    "pressure-sealevel",
    "temperature-2m",
    "wind-speed-100m",
    "wind-direction-100m",
    "total-cloud-cover",
]

# Alle geografiske punkter vi bruker
ALL_POINTS = [
    # Ispunkter
    {"name": "ice:source", "lon": -42.4, "lat": 69.0, "group": "ICE"},
    {"name": "ice:mid",    "lon": -41.3, "lat": 68.6, "group": "ICE"},
    {"name": "ice:mouth",  "lon": -40.3, "lat": 68.2, "group": "ICE"},
    # Havpunkter
    {"name": "sea:W1",     "lon": -34.9, "lat": 62.8, "group": "SEA"},
    {"name": "sea:W2",     "lon": -33.7, "lat": 63.8, "group": "SEA"},
    {"name": "sea:W3",     "lon": -32.5, "lat": 64.8, "group": "SEA"},
    {"name": "sea:C1",     "lon": -30.5, "lat": 64.6, "group": "SEA"},
    {"name": "sea:C2",     "lon": -31.1, "lat": 65.4, "group": "SEA"},
    {"name": "sea:M1",     "lon": -30.1, "lat": 63.9, "group": "SEA"},
    {"name": "sea:M2",     "lon": -31.8, "lat": 64.3, "group": "SEA"},
    {"name": "sea:M3",     "lon": -30.2, "lat": 65.0, "group": "SEA"},
    {"name": "sea:E1",     "lon": -29.1, "lat": 65.8, "group": "SEA"},
    {"name": "sea:E2",     "lon": -27.5, "lat": 66.4, "group": "SEA"},
    # Kystpunkter
    {"name": "coast:K1",   "lon": -38.8, "lat": 65.7, "group": "COAST"},
    {"name": "coast:K2",   "lon": -37.6, "lat": 66.5, "group": "COAST"},
]

SEP = "-" * 60


def ok(msg):
    print(f"  ✓  {msg}")


def fail(msg):
    print(f"  ✗  {msg}")


def warn(msg):
    print(f"  ?  {msg}")


# ── Steg 1: Hent siste instans ────────────────────────────────────────────────

def get_latest_instance():
    print(SEP)
    print("STEG 1: Henter liste over tilgjengelige instanser")
    print(SEP)
    url = f"{BASE}/collections/{COL}/instances"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        fail(f"Klarte ikke hente instansliste: {e}")
        return None

    ids = []
    if isinstance(data, dict):
        for item in data.get("instances", []):
            iid = item.get("id") or item.get("instanceId")
            if iid:
                ids.append(iid)

    if not ids:
        fail("Ingen instanser funnet i responsen")
        return None

    ids = sorted(set(ids))
    print(f"  Fant {len(ids)} instanser. Nyeste: {ids[-1]}")
    print(f"  Eldre tilgjengelig: {ids[-3] if len(ids) >= 3 else 'N/A'}")
    return ids[-1]


# ── Steg 2: Sjekk tilgjengelige parameternavn fra collection-metadata ─────────

def check_collection_parameters():
    print()
    print(SEP)
    print("STEG 2: Henter parameterliste fra collection-metadata")
    print(SEP)
    url = f"{BASE}/collections/{COL}"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        fail(f"Klarte ikke hente collection-metadata: {e}")
        return None

    param_names = set()
    for key in ["parameter_names", "parameters", "parameter-names"]:
        val = data.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    param_names.add(item)
                elif isinstance(item, dict):
                    n = item.get("name") or item.get("id")
                    if n:
                        param_names.add(n)
            break
        elif isinstance(val, dict):
            param_names.update(val.keys())
            break

    if not param_names:
        warn("Fant ingen parameterliste i collection-metadata")
        return None

    print(f"  API rapporterer {len(param_names)} parametere totalt")
    print()

    print("  --- Produksjonsparametere ---")
    for p in sorted(PARAMS_TO_TEST):
        if p in param_names:
            ok(f"{p}  — finnes i collection-metadata")
        else:
            fail(f"{p}  — IKKE funnet i collection-metadata")

    # Dump alle vind/sky-relaterte parametere
    keywords = ["wind", "cloud", "cover", "dir", "fraction"]
    relevant = sorted(p for p in param_names if any(k in p.lower() for k in keywords))
    if relevant:
        print()
        print(f"  --- Alle vind/sky-relaterte parametere i harmonie_ig_sf ({len(relevant)} stk) ---")
        for p in relevant:
            print(f"       {p}")

    # Dump alle parametere med "100" i navnet
    at100 = sorted(p for p in param_names if "100" in p)
    if at100:
        print()
        print(f"  --- Alle parametere med 100 i navnet ({len(at100)} stk) ---")
        for p in at100:
            print(f"       {p}")

    return param_names


# ── Steg 3: Test hvert parameternavn individuelt ──────────────────────────────

def test_parameters(instance_id):
    print()
    print(SEP)
    print(f"STEG 3: Tester hvert parameternavn individuelt (instans: {instance_id})")
    print(f"        Testpunkt: ice:source (lon=-42.4, lat=69.0)")
    print(SEP)

    results = {}
    test_lon, test_lat = -42.4, 69.0
    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"

    for param in PARAMS_TO_TEST:
        time.sleep(0.4)
        params = {
            "coords": f"POINT({test_lon} {test_lat})",
            "parameter-name": param,
            "crs": "crs84",
            "f": "CoverageJSON",
        }
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            status = r.status_code
            if status == 200:
                data = r.json()
                ranges = data.get("ranges", {})
                values = ranges.get(param, {}).get("values", [])
                numeric = [v for v in values if isinstance(v, (int, float)) and v == v]
                if numeric:
                    ok(f"{param:<30} HTTP 200 — {len(numeric)} numeriske verdier")
                else:
                    warn(f"{param:<30} HTTP 200 — men ingen numeriske verdier i responsen")
                results[param] = "ok"
            elif status == 400:
                fail(f"{param:<30} HTTP 400 — parameternavn ukjent eller ugyldig for denne instansen")
                results[param] = "400"
            elif status == 404:
                fail(f"{param:<30} HTTP 404 — instans eller punkt ikke funnet")
                results[param] = "404"
            elif status == 429:
                warn(f"{param:<30} HTTP 429 — rate limit nådd, stopper parametertesting")
                results[param] = "429"
                break
            elif status == 502:
                warn(f"{param:<30} HTTP 502 — DMI backend midlertidig nede")
                results[param] = "502"
            else:
                warn(f"{param:<30} HTTP {status}")
                results[param] = str(status)
        except requests.exceptions.ReadTimeout:
            warn(f"{param:<30} Timeout — DMI svarte ikke innen {TIMEOUT[1]}s")
            results[param] = "timeout"
        except Exception as e:
            fail(f"{param:<30} Feil: {type(e).__name__}: {e}")
            results[param] = "error"

    return results


# ── Steg 4: Test alle geografiske punkter ────────────────────────────────────

def test_geographic_points(instance_id):
    print()
    print(SEP)
    print(f"STEG 4: Tester alle geografiske punkter (instans: {instance_id})")
    print(f"        Bruker: pressure-sealevel (enkleste parameter)")
    print(SEP)

    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    results = {}

    for pt in ALL_POINTS:
        time.sleep(0.4)
        params = {
            "coords": f"POINT({pt['lon']} {pt['lat']})",
            "parameter-name": "pressure-sealevel",
            "crs": "crs84",
            "f": "CoverageJSON",
        }
        name = pt["name"]
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            status = r.status_code
            if status == 200:
                data = r.json()
                values = data.get("ranges", {}).get("pressure-sealevel", {}).get("values", [])
                numeric = [v for v in values if isinstance(v, (int, float)) and v == v]
                if numeric:
                    ok(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — OK, {len(numeric)} tidssteg")
                else:
                    warn(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — HTTP 200 men ingen data")
                results[name] = "ok"
            elif status == 400:
                fail(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — HTTP 400 (utenfor griddet?)")
                results[name] = "400"
            elif status == 404:
                fail(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — HTTP 404")
                results[name] = "404"
            elif status == 502:
                warn(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — HTTP 502 (DMI nede)")
                results[name] = "502"
            elif status == 429:
                warn(f"{name:<18} — Rate limit nådd, stopper punkttesting")
                results[name] = "429"
                break
            else:
                warn(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — HTTP {status}")
                results[name] = str(status)
        except requests.exceptions.ReadTimeout:
            warn(f"{name:<18} ({pt['lon']:>6}, {pt['lat']:>5})  — Timeout")
            results[name] = "timeout"
        except Exception as e:
            fail(f"{name:<18} Feil: {type(e).__name__}: {e}")
            results[name] = "error"

    return results


# ── Steg 5: Kombinert test — alle parametere på ett kall (som produksjon) ─────

def test_combined_ice_call(instance_id):
    print()
    print(SEP)
    print(f"STEG 5: Kombinert kall — alle ICE-parametere samtidig (som produksjon gjør)")
    print(SEP)

    url = f"{BASE}/collections/{COL}/instances/{instance_id}/position"
    ice_params = ["pressure-sealevel", "temperature-2m", "wind-speed-100m",
                  "wind-direction-100m", "total-cloud-cover"]

    for pt in [p for p in ALL_POINTS if p["group"] == "ICE"]:
        time.sleep(0.4)
        params = {
            "coords": f"POINT({pt['lon']} {pt['lat']})",
            "parameter-name": ",".join(ice_params),
            "crs": "crs84",
            "f": "CoverageJSON",
        }
        name = pt["name"]
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            status = r.status_code
            if status == 200:
                ranges = r.json().get("ranges", {})
                found = []
                missing = []
                for p in ice_params:
                    vals = ranges.get(p, {}).get("values", [])
                    numeric = [v for v in vals if isinstance(v, (int, float)) and v == v]
                    if numeric:
                        found.append(p)
                    else:
                        missing.append(p)
                if missing:
                    warn(f"{name}: HTTP 200, men mangler data for: {', '.join(missing)}")
                    for f_ in found:
                        ok(f"  {f_}")
                    for m in missing:
                        fail(f"  {m}  (ingen numeriske verdier)")
                else:
                    ok(f"{name}: Alle {len(ice_params)} parametere OK")
            elif status == 400:
                fail(f"{name}: HTTP 400 — én eller flere parametere ugyldig for dette kallet")
                # Finn hvilken parameter som feiler ved å halvere søket
                print(f"       Prøver å isolere årsaken...")
                for p in ice_params:
                    time.sleep(0.3)
                    single = dict(params)
                    single["parameter-name"] = p
                    try:
                        r2 = requests.get(url, params=single, timeout=TIMEOUT)
                        if r2.status_code == 200:
                            ok(f"    {p}  — OK alene")
                        else:
                            fail(f"    {p}  — HTTP {r2.status_code} alene")
                    except Exception as e2:
                        warn(f"    {p}  — Feil: {e2}")
            else:
                warn(f"{name}: HTTP {status}")
        except requests.exceptions.ReadTimeout:
            warn(f"{name}: Timeout på kombinert kall")
        except Exception as e:
            fail(f"{name}: {type(e).__name__}: {e}")


# ── Oppsummering ──────────────────────────────────────────────────────────────

def print_summary(param_results, point_results):
    print()
    print(SEP)
    print("OPPSUMMERING")
    print(SEP)

    if param_results:
        bad_params = [p for p, r in param_results.items() if r != "ok"]
        if bad_params:
            fail(f"Parametere med feil: {', '.join(bad_params)}")
        else:
            ok("Alle parametere svarte OK")

    if point_results:
        bad_points = [p for p, r in point_results.items() if r not in ("ok",)]
        timeout_points = [p for p, r in point_results.items() if r == "timeout"]
        if bad_points:
            if all(r in ("timeout", "502") for r in [point_results[p] for p in bad_points]):
                warn(f"DMI ser ut til å være nede — {len(bad_points)} punkter svarte ikke")
            else:
                fail(f"Punkter utenfor grid eller med feil: {', '.join(bad_points)}")
        else:
            ok("Alle geografiske punkter svarte OK")

    print()


# ── Hovedfunksjon ─────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  DMI HARMONIE IG — API Diagnostikkscript")
    print("  Skrivebeskyttet: berører ingen prosjektfiler")
    print("=" * 60)

    instance_id = get_latest_instance()
    if instance_id is None:
        print()
        print("Klarte ikke hente instansliste. DMI API kan være nede.")
        sys.exit(1)

    check_collection_parameters()
    param_results = test_parameters(instance_id)
    point_results = test_geographic_points(instance_id)
    test_combined_ice_call(instance_id)
    print_summary(param_results, point_results)


if __name__ == "__main__":
    main()
