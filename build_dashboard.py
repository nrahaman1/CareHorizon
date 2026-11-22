"""Build CareHorizon v2.0 — TIGER/Line 2026 Road-Network Service-Area Shrinkage & ACS 2024 5-Year Block-Group Dashboard.

Reads:
  - TIGER/Line 2026 US Counties (data/tiger2026/tl_2026_us_county.zip)
  - TIGER/Line 2026 State Block Group Polygons (data/tiger2026/bg/tl_2026_<st>_bg.zip)
  - TIGER/Line 2026 State Primary & Secondary Road Networks (data/tiger2026/prisecroads/tl_2026_<st>_prisecroads.zip)
  - 2024 ACS 5-Year (2020-2024) Block Group Age Table B01001 (data/tiger2026/acs2024_5yr_bg_age.parquet)
  - CONUS Hospitals (data/hospitals_conus.parquet) & Fire Stations (data/fire_stations_conus.parquet)

Computes:
  - Multi-source Dijkstra shortest-path routing over the TIGER/Line 2026 road network graph from all Acute Hospitals & Fire Stations
  - Exact road-network travel times (t_hosp, t_fire) and serving facilities for every TIGER/Line road segment and every 2026 Block Group
  - Per-state Block Group Polygon shards in data/states/bg_<st>.js for instant County Drill-Down polygon rendering (works on file:// and http://)
  - Compressed binary payload injected into dashboard_template.html -> emergency_service_dashboard.html
"""
from __future__ import annotations

import base64
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time
import zlib

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
TIGER_DIR = DATA_DIR / "tiger2026"
STATES_OUT_DIR = DATA_DIR / "states"

EQ = "EPSG:5070"
Q = 1000
CIRCUITY = 1.28

STATE_FIPS_TO_ABBR = {
    "01": "AL", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE", "11": "DC",
    "12": "FL", "13": "GA", "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO",
    "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC",
    "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
}
CONUS_FIPS = sorted(STATE_FIPS_TO_ABBR.keys())

DATA_SLOT = "/*__CARE_DATA__*/null"
CONFIG_SLOT = "/*__CONFIG__*/null"


def encode_poly(geom, q: int = Q) -> list[list[int]]:
    out = []
    for part in shapely.get_parts(geom):
        if part.geom_type not in ("Polygon", "LinearRing"):
            continue
        rings = [part.exterior, *part.interiors] if part.geom_type == "Polygon" else [part]
        for ring in rings:
            coords = np.round(shapely.get_coordinates(ring) * q).astype(np.int64)
            if len(coords) < 3:
                continue
            out.append(np.vstack([coords[:1], np.diff(coords, axis=0)]).ravel().tolist())
    return out


def encode_poly_exterior(geom, q: int = Q) -> list[list[int]]:
    """Compact exterior rings for Block Group drill-down polygons."""
    out = []
    for part in shapely.get_parts(geom):
        if part.geom_type != "Polygon":
            continue
        coords = np.round(shapely.get_coordinates(part.exterior) * q).astype(np.int64)
        if len(coords) < 3:
            continue
        diffs = np.diff(coords, axis=0)
        keep = np.ones(len(coords), dtype=bool)
        keep[1:] = (diffs[:, 0] != 0) | (diffs[:, 1] != 0)
        coords = coords[keep]
        if len(coords) < 3:
            continue
        out.append(np.vstack([coords[:1], np.diff(coords, axis=0)]).ravel().tolist())
    return out


def dict_encode(series: pd.Series) -> tuple[list[str], list[int]]:
    vals = series.fillna("N/A").astype(str).str.strip().replace("", "N/A")
    uniq, idx = np.unique(vals.values, return_inverse=True)
    return uniq.tolist(), idx.tolist()


def join_points_to_counties(xy_eq: np.ndarray, counties_eq: gpd.GeoDataFrame, max_dist: float = 25_000) -> np.ndarray:
    pts = gpd.GeoDataFrame(geometry=gpd.points_from_xy(xy_eq[:, 0], xy_eq[:, 1]), crs=EQ)
    polys = counties_eq[["geometry"]].assign(cidx=np.arange(len(counties_eq), dtype=np.int32))
    hit = gpd.sjoin(pts, polys, predicate="within", how="left")
    hit = hit[~hit.index.duplicated(keep="first")]
    res = hit["cidx"].fillna(-1).astype(np.int32).values
    miss = np.flatnonzero(res < 0)
    if len(miss):
        near = gpd.sjoin_nearest(pts.iloc[miss], polys, max_distance=max_dist, how="left")
        near = near[~near.index.duplicated(keep="first")]
        res[miss] = near["cidx"].fillna(-1).astype(np.int32).values
    miss2 = np.flatnonzero(res < 0)
    if len(miss2):
        cent = counties_eq.geometry.centroid
        tree = cKDTree(np.column_stack([cent.x.values, cent.y.values]))
        _, idx = tree.query(xy_eq[miss2], k=1)
        res[miss2] = idx.astype(np.int32)
    return res


def gzip_b64(raw_bytes: bytes) -> str:
    comp = zlib.compressobj(level=9, wbits=16 + zlib.MAX_WBITS)
    gz = comp.compress(raw_bytes) + comp.flush()
    return base64.b64encode(gz).decode("ascii")


def pack_columns(n: int, arrays: list[tuple[str, np.ndarray]]) -> tuple[int, str, dict]:
    n_pad = ((n + 3) // 4) * 4
    chunks = []
    schema = {}
    offset = 0
    for name, arr in arrays:
        pad_arr = np.zeros(n_pad, dtype=arr.dtype)
        pad_arr[:n] = arr
        raw = pad_arr.tobytes()
        chunks.append(raw)
        schema[name] = {"dtype": str(arr.dtype), "offset": offset, "bytes": len(raw)}
        offset += len(raw)
    return n_pad, gzip_b64(b"".join(chunks)), schema


def run_hybrid_dijkstra(
    n_nodes: int,
    u: np.ndarray,
    v: np.ndarray,
    w_min: np.ndarray,
    knn_u: np.ndarray,
    knn_v: np.ndarray,
    knn_w: np.ndarray,
    node_xy: np.ndarray,
    fac_xy: np.ndarray,
    fac_orig_idx: np.ndarray,
    access_speed_kmh: float = 58.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Multi-source Dijkstra over TIGER/Line highway graph + local collector connectors."""
    d_fac, rel_idx = cKDTree(fac_xy).query(node_xy, k=1)
    w_access = np.maximum(0.05, (d_fac * CIRCUITY / (access_speed_kmh / 3.6)) / 60.0)
    src = n_nodes
    rows = np.concatenate([u, v, knn_u, knn_v, np.full(n_nodes, src, dtype=np.int32)])
    cols = np.concatenate([v, u, knn_v, knn_u, np.arange(n_nodes, dtype=np.int32)])
    data = np.concatenate([w_min, w_min, knn_w, knn_w, w_access])
    G = csr_matrix((data, (rows, cols)), shape=(n_nodes + 1, n_nodes + 1))
    dist, pred = dijkstra(G, directed=True, indices=src, return_predecessors=True)
    dist = dist[:n_nodes]

    root = np.arange(n_nodes, dtype=np.int32)
    for _ in range(36):
        p = pred[root]
        mask = (p >= 0) & (p != src)
        if not mask.any():
            break
        root[mask] = p[mask]
    node_fac = fac_orig_idx[rel_idx[root]]
    return dist, node_fac


def process_one_state(args: tuple) -> dict:
    """Worker for a single CONUS state: loads TIGER 2026 BG + PRISECROADS, runs Dijkstra, writes BG polygon shard."""
    (
        st,
        bg_zip_path,
        rd_zip_path,
        acs_sub_df,
        fips5_to_cty,
        cty_cent_xy,
        h_ac_xy,
        h_ac_orig_idx,
        f_xy,
        states_out_dir,
    ) = args

    # 1. Load TIGER/Line 2026 Block Groups for state `st`
    bg = gpd.read_file(bg_zip_path)
    bg = bg[bg.geometry.notna() & ~bg.geometry.is_empty].reset_index(drop=True)
    bg["GEOID"] = bg["GEOID"].astype(str).str.strip()
    bg["lon"] = pd.to_numeric(bg["INTPTLON"], errors="coerce")
    bg["lat"] = pd.to_numeric(bg["INTPTLAT"], errors="coerce")
    bg["aland_km2"] = np.maximum(pd.to_numeric(bg["ALAND"], errors="coerce").fillna(1e6) / 1e6, 0.01)

    bg = bg.merge(acs_sub_df, on="GEOID", how="left")
    for col in ["pop", "a0", "a1", "a2", "a3", "a4", "a5"]:
        bg[col] = bg[col].fillna(0).clip(0, 65535).astype(np.uint16)

    # Sort deterministically by GEOID
    bg = bg.sort_values("GEOID").reset_index(drop=True)
    n_bg = len(bg)

    bg_eq_geom = bg.geometry.to_crs(EQ)
    bg_pts = gpd.GeoSeries.from_xy(bg["lon"], bg["lat"], crs=4326).to_crs(EQ)
    bg_xy = np.column_stack([bg_pts.x.values, bg_pts.y.values])

    # Map STATEFP + COUNTYFP -> 2026 County Index
    fips5_strs = (bg["STATEFP"].astype(str).str.zfill(2) + bg["COUNTYFP"].astype(str).str.zfill(3)).tolist()
    cty_tree = cKDTree(cty_cent_xy)
    bg_cty = np.empty(n_bg, dtype=np.uint16)
    for i, f5 in enumerate(fips5_strs):
        cidx = fips5_to_cty.get(f5, -1)
        if cidx < 0:
            _, cidx = cty_tree.query(bg_xy[i], k=1)
        bg_cty[i] = int(cidx)

    # Simplify & encode TIGER/Line 2026 Block Group polygons for County Drill-Down
    simp_wgs = bg_eq_geom.simplify(130).to_crs(4326).values
    poly_geoms = [encode_poly_exterior(g, Q) for g in simp_wgs]

    # 2. Load TIGER/Line 2026 Primary & Secondary Roads (`tl_2026_<st>_prisecroads.zip`)
    rd = gpd.read_file(rd_zip_path)
    rd = rd[rd.geometry.notna() & ~rd.geometry.is_empty].explode(index_parts=False).reset_index(drop=True)
    rd_eq = rd.geometry.to_crs(EQ).values
    len_m = shapely.length(rd_eq)

    p0 = shapely.get_coordinates(shapely.get_point(rd_eq, 0))
    p1 = shapely.get_coordinates(shapely.get_point(rd_eq, -1))
    all_pts = np.vstack([p0, p1])
    q_pts = np.round(all_pts / 100.0).astype(np.int64)
    uniq_q, inv = np.unique(q_pts, axis=0, return_inverse=True)
    n_nodes = len(uniq_q)
    u = inv[:len(rd_eq)].astype(np.int32)
    v = inv[len(rd_eq):].astype(np.int32)
    node_xy = uniq_q.astype(np.float64) * 100.0

    is_prim = (rd["MTFCC"].values == "S1100")
    sp_mps = np.where(is_prim, 105.0 / 3.6, 78.0 / 3.6)
    w_min = np.maximum(0.01, (len_m / sp_mps) / 60.0)

    tree_n = cKDTree(node_xy)
    k_use = min(4, n_nodes)
    d_knn, idx_knn = tree_n.query(node_xy, k=k_use)
    if k_use > 1:
        knn_u = np.repeat(np.arange(n_nodes, dtype=np.int32), k_use - 1)
        knn_v = idx_knn[:, 1:].ravel().astype(np.int32)
        knn_w = np.maximum(0.02, (d_knn[:, 1:].ravel() * CIRCUITY / (52.0 / 3.6)) / 60.0)
    else:
        knn_u = np.empty(0, dtype=np.int32); knn_v = np.empty(0, dtype=np.int32); knn_w = np.empty(0, dtype=np.float64)

    f_orig_idx = np.arange(len(f_xy), dtype=np.int32)
    dist_h, node_hosp = run_hybrid_dijkstra(n_nodes, u, v, w_min, knn_u, knn_v, knn_w, node_xy, h_ac_xy, h_ac_orig_idx, 58.0)
    dist_f, node_fire = run_hybrid_dijkstra(n_nodes, u, v, w_min, knn_u, knn_v, knn_w, node_xy, f_xy, f_orig_idx, 56.0)

    # 3. Route Block Groups over TIGER/Line 2026 Road Network Graph
    d_bg_node, bg_near_node = tree_n.query(bg_xy, k=1)
    d_bg_h_dir, bg_h_dir_rel = cKDTree(h_ac_xy).query(bg_xy, k=1)
    d_bg_f_dir, bg_f_dir_idx = cKDTree(f_xy).query(bg_xy, k=1)

    bg_pop_f = (
        bg["a0"].astype(np.float64) + bg["a1"].astype(np.float64)
        + bg["a2"].astype(np.float64) + bg["a3"].astype(np.float64) + bg["a4"].astype(np.float64)
    ).values
    dens = bg_pop_f / bg["aland_km2"].values
    v_local_kmh = np.clip(84.0 - 8.5 * np.log10(np.maximum(dens, 1.0)), 46.0, 84.0)
    v_local_mps = v_local_kmh / 3.6

    t_conn_min = ((d_bg_node * CIRCUITY) / v_local_mps) / 60.0
    t_h_net = dist_h[bg_near_node] + t_conn_min
    t_h_dir = ((d_bg_h_dir * CIRCUITY) / v_local_mps) / 60.0
    use_h_net = t_h_net <= t_h_dir
    t_hosp_min = np.where(use_h_net, t_h_net, t_h_dir)
    bg_hosp = np.where(use_h_net, node_hosp[bg_near_node], h_ac_orig_idx[bg_h_dir_rel]).astype(np.uint16)

    t_f_net = dist_f[bg_near_node] + t_conn_min
    t_f_dir = ((d_bg_f_dir * CIRCUITY) / v_local_mps) / 60.0
    use_f_net = t_f_net <= t_f_dir
    t_fire_min = np.where(use_f_net, t_f_net, t_f_dir)
    bg_fire = np.where(use_f_net, node_fire[bg_near_node], bg_f_dir_idx).astype(np.uint16)

    # 4. Assign Road Segments to nearest Block Group & County, and filter segments >= 250m for rendering
    mid_xy = 0.5 * (p0 + p1)
    _, edge_bg_local = cKDTree(bg_xy).query(mid_xy, k=1)
    edge_km = len_m / 1000.0

    # Accumulate highway/arterial road km per Block Group + estimate local street grid km from ALAND & density
    bg_hwy_km = np.bincount(edge_bg_local, weights=edge_km, minlength=n_bg)
    bg_local_km = np.sqrt(bg["aland_km2"].values) * np.clip(1.8 + 0.6 * np.log10(np.maximum(dens, 1.0)), 1.5, 6.0)
    bg_road_km_10 = np.clip(np.round((bg_hwy_km + bg_local_km) * 10.0), 1, 65535).astype(np.uint16)

    keep = len_m >= 250.0
    rd_keep_eq = rd_eq[keep]
    u_k = u[keep]; v_k = v[keep]
    edge_bg_k = edge_bg_local[keep].astype(np.uint32)
    edge_cty_k = bg_cty[edge_bg_k]
    edge_th_k = np.clip(np.round(0.5 * (dist_h[u_k] + dist_h[v_k]) * 10.0), 1, 65535).astype(np.uint16)
    edge_tf_k = np.clip(np.round(0.5 * (dist_f[u_k] + dist_f[v_k]) * 10.0), 1, 65535).astype(np.uint16)
    edge_hosp_k = node_hosp[u_k].astype(np.uint16)
    edge_fire_k = node_fire[u_k].astype(np.uint16)
    edge_km10_k = np.clip(np.round(edge_km[keep] * 10.0), 1, 65535).astype(np.uint16)
    edge_mtfcc_k = np.where(is_prim[keep], 1, 2).astype(np.uint8)
    raw_names = rd["FULLNAME"].fillna("").astype(str).str.strip().values[keep] if "FULLNAME" in rd.columns else np.full(int(keep.sum()), "", dtype=object)

    simp_rd_wgs = gpd.GeoSeries(rd_keep_eq, crs=EQ).simplify(240).to_crs(4326).values
    rd_xy_chunks = []
    rd_vcount = []
    valid_rd_mask = []
    for g in simp_rd_wgs:
        pts = np.round(shapely.get_coordinates(g) * Q).astype(np.int32)
        if len(pts) < 2:
            valid_rd_mask.append(False)
            continue
        valid_rd_mask.append(True)
        delta = np.vstack([pts[:1], np.diff(pts, axis=0)]).ravel().astype(np.int32)
        rd_xy_chunks.append(delta)
        rd_vcount.append(len(pts))

    vmask = np.array(valid_rd_mask, dtype=bool)
    edge_names_k = [
        (nm if nm else ("Interstate Hwy" if mt == 1 else "Secondary Hwy")).replace("\n", " ").replace("\t", " ")
        for nm, mt in zip(raw_names[vmask], edge_mtfcc_k[vmask])
    ]
    return {
        "st": st,
        "n_bg": n_bg,
        "poly_geoms": poly_geoms,
        "bg": {
            "lon_q": np.clip(np.round((bg["lon"].values + 130.0) * 1000.0), 0, 65535).astype(np.uint16),
            "lat_q": np.clip(np.round((bg["lat"].values - 24.0) * 2000.0), 0, 65535).astype(np.uint16),
            "cty": bg_cty,
            "hosp": bg_hosp,
            "fire": bg_fire,
            "t_hosp": np.clip(np.round(t_hosp_min * 10.0), 1, 65535).astype(np.uint16),
            "t_fire": np.clip(np.round(t_fire_min * 10.0), 1, 65535).astype(np.uint16),
            "road_km10": bg_road_km_10,
            "v_base": np.round(v_local_kmh).astype(np.uint8),
            "a0": bg["a0"].values,
            "a1": bg["a1"].values,
            "a2": bg["a2"].values,
            "a3": bg["a3"].values,
            "a4": bg["a4"].values,
            "a5": bg["a5"].values,
            "fips5": (bg["STATEFP"].astype(int) * 1000 + bg["COUNTYFP"].astype(int)).astype(np.uint16).values,
            "tract_bg": (bg["TRACTCE"].astype(int) * 10 + bg["BLKGRPCE"].astype(int)).astype(np.uint32).values,
        },
        "rd": {
            "xy_chunks": rd_xy_chunks,
            "vcount": np.array(rd_vcount, dtype=np.uint16),
            "cty": edge_cty_k[vmask],
            "bg_local": edge_bg_k[vmask],
            "hosp": edge_hosp_k[vmask],
            "fire": edge_fire_k[vmask],
            "t_hosp": edge_th_k[vmask],
            "t_fire": edge_tf_k[vmask],
            "km10": edge_km10_k[vmask],
            "mtfcc": edge_mtfcc_k[vmask],
            "names": edge_names_k,
        },
    }


def main():
    t_start = time.time()
    STATES_OUT_DIR.mkdir(parents=True, exist_ok=True)
    template_path = HERE / "dashboard_template.html"
    out_path = HERE / "emergency_service_dashboard.html"
    config_path = HERE / "carehorizon.config.json"

    html = template_path.read_text(encoding="utf-8")
    if html.count(DATA_SLOT) != 1 or html.count(CONFIG_SLOT) != 1:
        sys.exit(f"{template_path.name} must contain {DATA_SLOT} and {CONFIG_SLOT}.")

    # 1. Load TIGER/Line 2026 CONUS Counties
    print("1. Loading TIGER/Line 2026 CONUS counties...", flush=True)
    cty = gpd.read_file(TIGER_DIR / "tl_2026_us_county.zip")
    cty = cty[cty["STATEFP"].isin(CONUS_FIPS) & cty.geometry.notna()].copy()
    cty["ST"] = cty["STATEFP"].map(STATE_FIPS_TO_ABBR)
    cty = cty.sort_values("GEOID").reset_index(drop=True)
    cty_eq = cty.to_crs(EQ)
    cty_cent = cty_eq.geometry.centroid
    cty_cent_xy = np.column_stack([cty_cent.x.values, cty_cent.y.values])
    fips5_to_cty = {str(g).zfill(5): idx for idx, g in enumerate(cty["GEOID"])}
    cty_geom = [encode_poly(g, Q) for g in cty_eq.geometry.simplify(1200).to_crs(4326).values]
    print(f"   {len(cty):,} CONUS TIGER/Line 2026 counties loaded ({time.time()-t_start:.1f}s)", flush=True)

    # 2. Load CONUS Hospitals & Fire Stations
    print("2. Loading CONUS hospitals & fire stations...", flush=True)
    hdf = pd.read_parquet(DATA_DIR / "hospitals_conus.parquet")
    h_pts = gpd.GeoSeries.from_xy(hdf["lon"], hdf["lat"], crs=4326).to_crs(EQ)
    h_xy = np.column_stack([h_pts.x.values, h_pts.y.values])
    h_cty = join_points_to_counties(h_xy, cty_eq)
    acute_types = {"GENERAL ACUTE CARE", "CRITICAL ACCESS", "CHILDREN", "MILITARY"}
    has_trauma = ~hdf["TRAUMA"].astype(str).str.upper().str.strip().isin(["NOT AVAILABLE", "", "NONE", "NAN", "N/A"])
    h_acute = (hdf["TYPE"].astype(str).str.upper().str.strip().isin(acute_types) | has_trauma).astype(np.uint8).values
    h_ac_orig_idx = np.flatnonzero(h_acute == 1).astype(np.int32)
    h_ac_xy = h_xy[h_ac_orig_idx]

    h_type_dict, h_type_idx = dict_encode(hdf["TYPE"])
    h_trauma_dict, h_trauma_idx = dict_encode(hdf["TRAUMA"].astype(str).str.replace("LEVEL ", "Lvl ", regex=False))
    h_st_dict, h_st_idx = dict_encode(hdf["STATE"])
    h_helipad = (hdf["HELIPAD"].astype(str).str.upper().str.strip() == "Y").astype(np.uint8).values

    fdf = pd.read_parquet(DATA_DIR / "fire_stations_conus.parquet")
    f_pts = gpd.GeoSeries.from_xy(fdf["lon"], fdf["lat"], crs=4326).to_crs(EQ)
    f_xy = np.column_stack([f_pts.x.values, f_pts.y.values])
    f_cty = join_points_to_counties(f_xy, cty_eq)
    f_ems = (fdf["EMS"].astype(str).str.upper().str.strip() == "Y").astype(np.uint8).values

    # 3. Load 2024 ACS 5-Year Block Group Age Data
    print("3. Loading 2024 ACS 5-Year (2020-2024) Block Group age dataset...", flush=True)
    acs_df = pd.read_parquet(TIGER_DIR / "acs2024_5yr_bg_age.parquet")
    acs_df["GEOID"] = acs_df["GEOID"].astype(str).str.strip()
    acs_by_st = {st: grp.copy() for st, grp in acs_df.groupby(acs_df["GEOID"].str[:2])}

    # 4. Process all 49 CONUS States in parallel (TIGER 2026 BG + TIGER 2026 PRISECROADS + Dijkstra)
    print("4. Routing TIGER/Line 2026 Road Networks & Block Groups across 49 CONUS states...", flush=True)
    worker_args = []
    for st in CONUS_FIPS:
        worker_args.append((
            st,
            TIGER_DIR / "bg" / f"tl_2026_{st}_bg.zip",
            TIGER_DIR / "prisecroads" / f"tl_2026_{st}_prisecroads.zip",
            acs_by_st.get(st, pd.DataFrame(columns=["GEOID", "pop", "a0", "a1", "a2", "a3", "a4", "a5"])),
            fips5_to_cty,
            cty_cent_xy,
            h_ac_xy,
            h_ac_orig_idx,
            f_xy,
            STATES_OUT_DIR,
        ))

    state_results = {}
    with ProcessPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(process_one_state, wa): wa[0] for wa in worker_args}
        done = 0
        for fut in as_completed(futs):
            res = fut.result()
            state_results[res["st"]] = res
            done += 1
            if done % 10 == 0 or done == len(CONUS_FIPS):
                print(f"   [{done}/{len(CONUS_FIPS)} states routed] ({time.time()-t_start:.1f}s)", flush=True)

    # 5. Assemble Ordered National Arrays & Write Per-State Block Group Polygon Shards
    print("5. Packing TIGER/Line 2026 Road Network, Block Groups, and State Polygon Shards...", flush=True)
    bg_cols = {k: [] for k in ["lon_q", "lat_q", "cty", "hosp", "fire", "t_hosp", "t_fire", "road_km10", "v_base", "a0", "a1", "a2", "a3", "a4", "a5", "fips5", "tract_bg"]}
    rd_cols = {k: [] for k in ["vcount", "cty", "bg", "hosp", "fire", "t_hosp", "t_fire", "km10", "mtfcc", "name_id"]}
    rd_xy_all = []
    rd_name_map = {}
    rd_name_list = []

    bg_offset = 0
    state_bg_ranges = {}
    for st in CONUS_FIPS:
        res = state_results[st]
        n_bg_st = res["n_bg"]
        state_bg_ranges[st] = [bg_offset, bg_offset + n_bg_st]

        # Write state Block Group polygon shard (data/states/bg_<st>.js)
        shard_obj = {"st": st, "start": bg_offset, "n": n_bg_st, "geom": res["poly_geoms"]}
        shard_js = f'window.__BG_POLY=window.__BG_POLY||{{}};window.__BG_POLY["{st}"]={json.dumps(shard_obj, separators=(",", ":"))};'
        (STATES_OUT_DIR / f"bg_{st}.js").write_text(shard_js, encoding="utf-8")

        for k in bg_cols:
            bg_cols[k].append(res["bg"][k])

        rd = res["rd"]
        rd_xy_all.extend(rd["xy_chunks"])
        rd_cols["vcount"].append(rd["vcount"])
        rd_cols["cty"].append(rd["cty"])
        rd_cols["bg"].append(rd["bg_local"] + bg_offset)
        rd_cols["hosp"].append(rd["hosp"])
        rd_cols["fire"].append(rd["fire"])
        rd_cols["t_hosp"].append(rd["t_hosp"])
        rd_cols["t_fire"].append(rd["t_fire"])
        rd_cols["km10"].append(rd["km10"])
        rd_cols["mtfcc"].append(rd["mtfcc"])
        nids = np.empty(len(rd["names"]), dtype=np.uint16)
        for idx_n, nm in enumerate(rd["names"]):
            nid = rd_name_map.get(nm)
            if nid is None:
                nid = len(rd_name_list)
                if nid < 65535:
                    rd_name_map[nm] = nid
                    rd_name_list.append(nm)
                else:
                    nid = 0
            nids[idx_n] = nid
        rd_cols["name_id"].append(nids)

        bg_offset += n_bg_st

    bg_merged = {k: np.concatenate(v) for k, v in bg_cols.items()}
    rd_merged = {k: np.concatenate(v) for k, v in rd_cols.items()}
    rd_xy_flat = np.concatenate(rd_xy_all).astype(np.int32)
    n_bg_tot = bg_offset
    n_rd_tot = len(rd_merged["vcount"])

    print(f"   Total CONUS 2026 Block Groups: {n_bg_tot:,} | Total TIGER/Line 2026 Road Segments: {n_rd_tot:,} ({len(rd_xy_flat)//2:,} vertices)", flush=True)

    # Compute 2024 ACS 5-Year County Populations & Road Network Km from Block Groups & Roads
    bg_pop_arr = (
        bg_merged["a0"].astype(np.int64) + bg_merged["a1"].astype(np.int64)
        + bg_merged["a2"].astype(np.int64) + bg_merged["a3"].astype(np.int64) + bg_merged["a4"].astype(np.int64)
    )
    cty_pop = np.bincount(bg_merged["cty"], weights=bg_pop_arr, minlength=len(cty)).astype(int).tolist()
    cty_bg_cnt = np.bincount(bg_merged["cty"], minlength=len(cty)).tolist()
    cty_hosp_cnt = np.bincount(h_cty, minlength=len(cty)).tolist()
    cty_fire_cnt = np.bincount(f_cty, minlength=len(cty)).tolist()
    cty_road_km = np.round(np.bincount(rd_merged["cty"], weights=rd_merged["km10"] * 0.1, minlength=len(cty)), 1).tolist()

    # Pack Block Groups binary blob
    b_pad, b_blob, b_schema = pack_columns(n_bg_tot, [
        ("lon_q", bg_merged["lon_q"]),
        ("lat_q", bg_merged["lat_q"]),
        ("cty", bg_merged["cty"]),
        ("hosp", bg_merged["hosp"]),
        ("fire", bg_merged["fire"]),
        ("t_hosp", bg_merged["t_hosp"]),
        ("t_fire", bg_merged["t_fire"]),
        ("road_km10", bg_merged["road_km10"]),
        ("v_base", bg_merged["v_base"]),
        ("a0", bg_merged["a0"]),
        ("a1", bg_merged["a1"]),
        ("a2", bg_merged["a2"]),
        ("a3", bg_merged["a3"]),
        ("a4", bg_merged["a4"]),
        ("a5", bg_merged["a5"]),
        ("fips5", bg_merged["fips5"]),
        ("tract_bg", bg_merged["tract_bg"]),
    ])

    # Pack TIGER/Line 2026 Road Network attributes + XY coordinate stream + Road Name Dictionary
    r_pad, r_blob, r_schema = pack_columns(n_rd_tot, [
        ("vcount", rd_merged["vcount"]),
        ("cty", rd_merged["cty"]),
        ("bg", rd_merged["bg"]),
        ("hosp", rd_merged["hosp"]),
        ("fire", rd_merged["fire"]),
        ("t_hosp", rd_merged["t_hosp"]),
        ("t_fire", rd_merged["t_fire"]),
        ("km10", rd_merged["km10"]),
        ("mtfcc", rd_merged["mtfcc"]),
        ("name_id", rd_merged["name_id"]),
    ])
    r_xy_blob = gzip_b64(rd_xy_flat.tobytes())
    r_names_blob = gzip_b64("\n".join(rd_name_list).encode("utf-8"))

    # Pack Fire Stations
    f_lon_q = np.clip(np.round((fdf["lon"].values + 130.0) * 1000.0), 0, 65535).astype(np.uint16)
    f_lat_q = np.clip(np.round((fdf["lat"].values - 24.0) * 2000.0), 0, 65535).astype(np.uint16)
    f_pad, f_blob, f_schema = pack_columns(len(fdf), [
        ("lon_q", f_lon_q),
        ("lat_q", f_lat_q),
        ("cty", f_cty.astype(np.uint16)),
        ("ems", f_ems),
    ])
    f_labels_txt = "\n".join(
        f"{str(n or 'Fire Station').strip().replace(chr(9), ' ').replace(chr(10), ' ')}\t{str(c or '').strip()}, {str(s or '').strip()}"
        for n, c, s in zip(fdf["NAME"], fdf["CITY"], fdf["STATE"])
    )

    payload = {
        "q": Q,
        "meta": {
            "built": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "circuity": CIRCUITY,
            "sources": {
                "roads": f"{n_rd_tot:,} TIGER/Line 2026 Primary & Secondary Road corridors ({int(rd_merged['km10'].sum()*0.1):,} km routed via Dijkstra)",
                "blockGroups": f"{n_bg_tot:,} TIGER/Line 2026 Block Groups + 2024 ACS 5-Year (2020-2024) B01001 Age Cohorts",
                "counties": f"{len(cty):,} TIGER/Line 2026 CONUS Counties",
                "hospitals": f"{len(hdf):,} CONUS open hospitals ({int(h_acute.sum()):,} acute/ER/trauma)",
                "fire": f"{len(fdf):,} CONUS fire stations ({int(f_ems.sum()):,} EMS)",
            },
        },
        "counties": {
            "n": int(len(cty)),
            "name": cty["NAME"].astype(str).tolist(),
            "st": cty["ST"].astype(str).tolist(),
            "stFips": cty["STATEFP"].astype(str).tolist(),
            "fips": cty["GEOID"].astype(str).tolist(),
            "pop": cty_pop,
            "bgCnt": cty_bg_cnt,
            "hospCnt": cty_hosp_cnt,
            "fireCnt": cty_fire_cnt,
            "roadKm": cty_road_km,
            "geom": cty_geom,
        },
        "hospitals": {
            "n": int(len(hdf)),
            "lon": np.round(hdf["lon"].values, 4).tolist(),
            "lat": np.round(hdf["lat"].values, 4).tolist(),
            "name": hdf["NAME"].fillna("Unknown Hospital").astype(str).str.strip().tolist(),
            "city": hdf["CITY"].fillna("").astype(str).str.strip().tolist(),
            "st": h_st_idx,
            "type": h_type_idx,
            "trauma": h_trauma_idx,
            "beds": hdf["BEDS"].fillna(-1).astype(int).tolist(),
            "heli": h_helipad.tolist(),
            "acute": h_acute.tolist(),
            "cty": h_cty.tolist(),
            "dicts": {"st": h_st_dict, "type": h_type_dict, "trauma": h_trauma_dict},
        },
        "fire": {
            "n": int(len(fdf)),
            "pad": f_pad,
            "blob": f_blob,
            "schema": f_schema,
            "labels": gzip_b64(f_labels_txt.encode("utf-8")),
        },
        "bg": {
            "n": int(n_bg_tot),
            "pad": b_pad,
            "blob": b_blob,
            "schema": b_schema,
            "totPop": int(bg_pop_arr.sum()),
            "pop75Plus": int(bg_merged["a5"].sum()),
            "stateRanges": state_bg_ranges,
        },
        "roads": {
            "n": int(n_rd_tot),
            "pad": r_pad,
            "blob": r_blob,
            "schema": r_schema,
            "xyBlob": r_xy_blob,
            "namesBlob": r_names_blob,
            "nCoords": int(len(rd_xy_flat)),
            "totKm": int(round(rd_merged["km10"].sum() * 0.1)),
        },
    }

    config_json = "null"
    if config_path.exists():
        config_json = json.dumps(json.loads(config_path.read_text(encoding="utf-8")), separators=(",", ":"))

    safe = lambda s: s.replace("</", "<\\/")
    out_html = html.replace(DATA_SLOT, safe(json.dumps(payload, separators=(",", ":")))).replace(CONFIG_SLOT, safe(config_json))
    out_path.write_text(out_html, encoding="utf-8")

    print(
        f"CareHorizon v2.0 built in {time.time()-t_start:.1f}s -> {out_path.name} ({out_path.stat().st_size/1e6:.2f} MB)\n"
        f"  Payload: BG={len(b_blob)/1e6:.2f}MB, Roads attr={len(r_blob)/1e6:.2f}MB + xy={len(r_xy_blob)/1e6:.2f}MB, "
        f"Fire={(len(f_blob)+len(payload['fire']['labels']))/1e6:.2f}MB, + 49 State BG Polygon Shards in data/states/"
    )


if __name__ == "__main__":
    main()
