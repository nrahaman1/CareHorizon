"""Download TIGER/Line 2026 CONUS Block Groups (BG) and Primary/Secondary Roads (PRISECROADS).

Saves into:
  data/tiger2026/bg/tl_2026_<st>_bg.zip
  data/tiger2026/prisecroads/tl_2026_<st>_prisecroads.zip
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time
import requests

HERE = Path(__file__).resolve().parent
TIGER_DIR = HERE / "data" / "tiger2026"
BG_DIR = TIGER_DIR / "bg"
RD_DIR = TIGER_DIR / "prisecroads"

CONUS_FIPS = [
    "01", "04", "05", "06", "08", "09", "10", "11", "12", "13",
    "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
    "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "36", "37", "38", "39", "40", "41", "42", "44", "45", "46",
    "47", "48", "49", "50", "51", "53", "54", "55", "56",
]


def download_one(url: str, dest: Path) -> tuple[str, float]:
    if dest.exists() and dest.stat().st_size > 10_000:
        return dest.name, dest.stat().st_size / 1e6
    for attempt in range(4):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(".tmp")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1 << 19):
                        f.write(chunk)
                tmp.replace(dest)
            return dest.name, dest.stat().st_size / 1e6
        except Exception:
            if attempt == 3:
                raise
            time.sleep(1.5 * (attempt + 1))
    return dest.name, 0.0


def main():
    BG_DIR.mkdir(parents=True, exist_ok=True)
    RD_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    tasks = []
    for st in CONUS_FIPS:
        tasks.append((
            f"https://www2.census.gov/geo/tiger/TIGER2026/PRISECROADS/tl_2026_{st}_prisecroads.zip",
            RD_DIR / f"tl_2026_{st}_prisecroads.zip",
        ))
        tasks.append((
            f"https://www2.census.gov/geo/tiger/TIGER2026/BG/tl_2026_{st}_bg.zip",
            BG_DIR / f"tl_2026_{st}_bg.zip",
        ))

    print(f"Downloading {len(tasks)} TIGER/Line 2026 state files (49 BG + 49 PRISECROADS)...", flush=True)
    tot_mb = 0.0
    done = 0
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = {pool.submit(download_one, u, d): d.name for u, d in tasks}
        for fut in as_completed(futs):
            name, mb = fut.result()
            tot_mb += mb
            done += 1
            if done % 10 == 0 or done == len(tasks):
                dt = time.time() - t0
                print(f"  [{done}/{len(tasks)}] {tot_mb:.1f} MB in {dt:.1f}s ({tot_mb/max(dt,0.1):.1f} MB/s)", flush=True)


if __name__ == "__main__":
    main()
