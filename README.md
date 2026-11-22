# CareHorizon — U.S. Emergency Service-Area Shrinkage Forecast

**Live Dashboard:** [https://nrahaman1.github.io/CareHorizon/](https://nrahaman1.github.io/CareHorizon/)

**CareHorizon** is an interactive, nationwide dashboard forecasting how severe weather (rain, snow, temperature, and wind) slows highway and street travel speeds and shrinks emergency response coverage for **acute hospitals** and **fire stations** across the **Contiguous United States (CONUS)**.

## Key Features

- **Real Road-Network Routing (CONUS + City Streets)**
  - **218,808 TIGER/Line 2026 Primary & Secondary road corridors** (`S1100`/`S1200`) with **55,263 unique road names**.
  - On-demand **city-level local street networks (`S1400`)** via Census TIGERweb when drilling into any county or zooming to city scale.
  - Clear visual distinction between **solid roads** and **dotted/dashed administrative boundaries** by level (County dash-dot, Selected County long-dash double-dot with pulsing neon glow, Block Group fine dots).
- **Nationwide Emergency Facilities & Demographics**
  - **5,513 acute/ER hospitals** (7,076 total) and **52,495 fire stations** routed via multi-source Dijkstra shortest paths.
  - **238,194 TIGER/Line 2026 Census Block Groups** paired with **2024 ACS 5-Year (`B01001`)** age cohorts (`0–4`, `5–17`, `18–34`, `35–64`, `65+`, `75+`).
- **Live Weather, Historical ERA5 & Storm Simulator**
  - **Live 72-Hour Forecast** via Open-Meteo API.
  - **Historical ERA5 Reanalysis** events (e.g., Winter Storm Uri, Hurricane Ida, Buffalo Blizzard).
  - **Offline Scenario Storm Simulator** for testing custom storm tracks anytime.
- **Interactive Analytics & Report Bar**
  - **Age & BGs**: Interactive labeled Donut chart of `No 15 min Access` population by age cohort, shrinkage-by-cohort chart, and sortable Block Group table.
  - **Hospitals, Fire Stns, Counties / BG Ranking, and Specific Road Ranking**: Click any chart bar or table row to fly smoothly to the feature with a pulsing map indicator; every column in every table supports ascending/descending sorting and one-click CSV export.
  - **Light & Dark Themes**: Friendly Google Material 3 Light default with a one-click header toggle to Twilight-Slate Dark mode, plus a built-in **8-step interactive tutorial** (`Tour`).

## Methodology

Under hourly ERA5 / Open-Meteo weather, road segments and block-group corridors experience empirical freeway/highway speed reduction $r$ (FHWA / Agarwal benchmarks), stretching network travel time to:

$$t_{\text{wx}} = \frac{t_{\text{base}}}{1 - r}$$

A Block Group or road corridor loses threshold access (**No 15 min Access** for hospitals / **No 8 min Access** for fire stations by default) when $t_{\text{base}} \le T < t_{\text{wx}}$.

## Local Development & Rebuild

```bash
pip install -r requirements.txt
python fetch_tiger2026.py
python fetch_facilities.py
python build_dashboard.py
```

Open `index.html` (or `emergency_service_dashboard.html`) directly in any modern browser.
