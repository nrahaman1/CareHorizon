"""Download and cache CONUS Hospitals and Fire Stations from public HIFLD ArcGIS REST endpoints.

Saves:
    data/hospitals_conus.parquet
    data/fire_stations_conus.parquet
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"

CONUS_STATES = {
    "AL", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

HOSP_URL = "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/Hospitals_hifld/FeatureServer/0"
HOSP_FALLBACK_URL = "https://services1.arcgis.com/CD5mKowwN6nIaqd8/arcgis/rest/services/HVRA_Source_Data_Hospitals/FeatureServer/0"
FIRE_URL = "https://services1.arcgis.com/wQnFk5ouCfPzTlPw/arcgis/rest/services/Fire_Stations/FeatureServer/0"


def fetch_page(session: requests.Session, url: str, fields: str, offset: int, page_size: int = 2000) -> list[dict]:
    params = {
        "where": "1=1",
        "outFields": fields,
        "returnGeometry": "true",
        "outSR": "4326",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "f": "json",
    }
    for attempt in range(4):
        try:
            r = session.get(f"{url}/query", params=params, timeout=30)
            r.raise_for_status()
            js = r.json()
            if "error" in js:
                raise RuntimeError(f"ArcGIS error at offset {offset}: {js['error']}")
            rows = []
            for feat in js.get("features", []):
                row = dict(feat.get("attributes") or {})
                geom = feat.get("geometry") or {}
                if "x" in geom and "y" in geom:
                    row["_gx"] = geom["x"]
                    row["_gy"] = geom["y"]
                rows.append(row)
            return rows
        except Exception:
            if attempt == 3:
                raise
            time.sleep(1.5 * (attempt + 1))
    return []


def fetch_all(url: str, fields: str, label: str, max_workers: int = 8) -> pd.DataFrame:
    session = requests.Session()
    cnt_js = session.get(f"{url}/query", params={"where": "1=1", "returnCountOnly": "true", "f": "json"}, timeout=20).json()
    total = int(cnt_js["count"])
    info = session.get(url, params={"f": "json"}, timeout=20).json()
    page_size = min(2000, int(info.get("maxRecordCount") or 1000))
    offsets = list(range(0, total, page_size))
    print(f"[{label}] Fetching {total:,} records in {len(offsets)} pages (page_size={page_size})...", flush=True)

    all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(fetch_page, session, url, fields, off, page_size): off for off in offsets}
        done = 0
        for fut in as_completed(futs):
            rows = fut.result()
            all_rows.extend(rows)
            done += 1
            if done % 10 == 0 or done == len(offsets):
                print(f"[{label}] {done}/{len(offsets)} pages ({len(all_rows):,} rows)", flush=True)
    return pd.DataFrame(all_rows)


def filter_conus(df: pd.DataFrame, lon_col: str, lat_col: str) -> pd.DataFrame:
    df = df.copy()
    df["STATE"] = df["STATE"].astype(str).str.strip().str.upper()
    df["lon"] = pd.to_numeric(df[lon_col], errors="coerce").fillna(pd.to_numeric(df.get("_gx"), errors="coerce"))
    df["lat"] = pd.to_numeric(df[lat_col], errors="coerce").fillna(pd.to_numeric(df.get("_gy"), errors="coerce"))
    mask = (
        df["STATE"].isin(CONUS_STATES)
        & df["lon"].between(-125.5, -66.5)
        & df["lat"].between(24.0, 50.0)
    )
    return df[mask].drop(columns=["_gx", "_gy"], errors="ignore").reset_index(drop=True)


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    hosp_fields = "ID,NAME,CITY,STATE,COUNTY,COUNTYFIPS,TYPE,STATUS,BEDS,TRAUMA,HELIPAD,LATITUDE,LONGITUDE"
    try:
        hdf = fetch_all(HOSP_URL, hosp_fields, "Hospitals", max_workers=6)
    except Exception as exc:
        print(f"[Hospitals] Primary endpoint failed ({exc}), trying HVRA fallback...")
        hdf = fetch_all(HOSP_FALLBACK_URL, hosp_fields, "Hospitals-Fallback", max_workers=6)

    hdf = filter_conus(hdf, "LONGITUDE", "LATITUDE")
    if "STATUS" in hdf.columns:
        open_mask = hdf["STATUS"].astype(str).str.upper().str.strip().isin(["OPEN", "NOT AVAILABLE", ""])
        if open_mask.sum() > 5000:
            hdf = hdf[open_mask].reset_index(drop=True)
    hdf["BEDS"] = pd.to_numeric(hdf["BEDS"], errors="coerce").fillna(-1).astype(int)
    hdf = hdf.drop_duplicates(subset=["ID", "NAME", "lon", "lat"]).reset_index(drop=True)
    hosp_out = DATA_DIR / "hospitals_conus.parquet"
    hdf.to_parquet(hosp_out, index=False)
    print(f"[Hospitals] Saved {len(hdf):,} CONUS hospitals -> {hosp_out}")

    fire_fields = "ID,NAME,CITY,STATE,COUNTY,FIPS,TYPE,EMS,X,Y"
    fdf = fetch_all(FIRE_URL, fire_fields, "FireStations", max_workers=10)
    fdf = filter_conus(fdf, "X", "Y")
    fdf = fdf.drop_duplicates(subset=["ID", "NAME", "lon", "lat"]).reset_index(drop=True)
    fire_out = DATA_DIR / "fire_stations_conus.parquet"
    fdf.to_parquet(fire_out, index=False)
    print(f"[FireStations] Saved {len(fdf):,} CONUS fire stations -> {fire_out}")


if __name__ == "__main__":
    main()
