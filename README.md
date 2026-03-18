# mrms-hail

Downloads NOAA MRMS MESHMax60min grib2 data from AWS S3 and builds a flat
Parquet file of CONUS hail pixels at 60-minute intervals. Includes a full
historical build (2021–present) and an incremental updater with gap-fill
and SQL delta output.

---

## What is MRMS MESH?

**MRMS** (Multi-Radar Multi-Sensor) is NOAA's real-time multi-sensor severe
weather analysis system. **MESH** (Maximum Expected Size of Hail) is a
radar-derived estimate of maximum hail size at the surface, measured in
millimeters.

**MESHMax60min** is the 60-minute rolling maximum MESH value — the largest
hail expected over the preceding 60 minutes at each 1 km grid cell. Updated
every 2 minutes operationally; this project samples the top-of-hour file
to produce one record per grid cell per hour.

- Grid: ~1 km resolution, CONUS coverage (-130° to -60°W, 20° to 55°N)
- Cells: 7,000 × 3,500 = 24.5 million per file
- Sentinel values: -1 (missing data), -3 (no radar coverage)
- Only cells with MESH > 0 mm are retained in the output

---

## Data Source

| Property | Value |
|----------|-------|
| Provider | NOAA / AWS Open Data |
| S3 Bucket | `s3://noaa-mrms-pds` (public, no auth) |
| HTTP path | `https://noaa-mrms-pds.s3.amazonaws.com/CONUS/MESH_Max_60min_00.50/` |
| Archive depth | October 2020 – present (continuous) |
| File format | grib2.gz (~40–55 KB compressed per file) |
| File naming | `MRMS_MESH_Max_60min_00.50_YYYYMMDD-HH0000.grib2.gz` |
| Update cadence | Every 2 minutes operationally; we sample top-of-hour only |

---

## Output Schema

All output files share the same four-column schema:

| Column | Type | Description |
|--------|------|-------------|
| `valid_time` | POSIXct UTC | Top-of-hour timestamp |
| `lon` | double | Longitude (°E, CONUS range ≈ -130 to -60) |
| `lat` | double | Latitude (°N, CONUS range ≈ 20 to 55) |
| `mesh_mm` | double | Max expected hail size in mm over preceding 60 min |

Only rows where `mesh_mm > 0` are written. A typical active severe weather
hour produces 5,000–50,000 rows; a quiet winter hour may produce fewer than
100.

---

## Project Structure

```
mrms-hail/
├── R/
│   ├── config.R            # All constants (paths, URLs, worker count)
│   ├── utils.R             # Core functions (download, parse, chunk, log)
│   └── install_packages.R  # One-time CRAN package setup
├── build_dataset.R         # Full historical build (2021 – present)
├── update_dataset.R        # Incremental updater with gap-fill + delta output
├── test_last_week.R        # Quick smoke test: last 7 days only
├── data/                   # Created at runtime, not tracked in git
│   ├── hail_mesh_60min.parquet   # Full dataset (main output)
│   ├── build_log.csv             # Per-hour status log
│   ├── chunks/                   # Monthly intermediate parquets (build only)
│   └── delta/                    # Per-run delta parquets for SQL INSERT
└── mrms-hail.Rproj
```

---

## Setup

### Requirements

- R 4.2+
- Windows, macOS, or Linux
- Internet access to `noaa-mrms-pds.s3.amazonaws.com`

### Install packages

```r
source("R/install_packages.R")
```

Installs: `httr2`, `terra`, `arrow`, `dplyr`, `lubridate`, `future`,
`furrr`, `fs`, `cli`, `readr`, `purrr`, `tibble`, `R.utils`

---

## Usage

### 1. Full historical build

```r
setwd("C:/claudegit/mrms-hail")
source("build_dataset.R")
```

Or from the command line:

```bash
Rscript build_dataset.R
```

**What it does:**

1. Generates every top-of-hour UTC timestamp from `2021-01-01` to the
   current hour
2. Checks `data/build_log.csv` and skips timestamps already completed
3. Downloads and parses one month at a time using parallel workers
4. Writes a chunk Parquet (`data/chunks/mesh_YYYY-MM.parquet`) after each
   month and appends to the build log — **safe to interrupt and resume**
5. Combines all chunks into `data/hail_mesh_60min.parquet`

**Performance** (12-core / 20-logical-thread machine, 6 workers, ~100 Mbps):

| Scope | Hours of data | Wall time |
|-------|--------------|-----------|
| 1 day | 24 | ~16 seconds |
| 1 week | 168 | ~2 minutes |
| 1 month | ~720 | ~8 minutes |
| **Full 5-year build** | **45,671** | **~8.5 hours** |

Throughput: **~0.67 seconds per hour-of-data** (6 parallel workers).
The bottleneck is `terra::values()` reading 98 MB of uncompressed raster
per file — memory bandwidth bound, not network bound.

**Resume:** If the build is interrupted, re-run `build_dataset.R`. It reads
the build log and skips all previously completed hours, resuming from where
it left off. At most one month of work is lost on interruption.

### 2. Incremental update

```r
source("update_dataset.R")
```

Run this on a schedule (daily, hourly, etc.) to bring the dataset current.

**What it does:**

1. Reads `data/hail_mesh_60min.parquet` and finds the most recent
   `valid_time`
2. Fetches all new top-of-hour files from `(max_time + 1h)` to the current
   UTC hour
3. Scans the build log for any timestamps previously logged as `missing` or
   `error` and retries them (gap-fill)
4. Appends new rows to the main Parquet
5. Writes `data/delta/hail_mesh_delta_YYYYMMDD_HHMMSS.parquet` containing
   **only the new rows** from this run (for SQL workflows — see below)

**Expected run time:**

| Update window | Processing | Parquet I/O | Total |
|---------------|------------|-------------|-------|
| 1 hour | ~1 sec | ~40–60 sec | ~1 min |
| 1 day (24 hrs) | ~16 sec | ~40–60 sec | **~1–2 min** |
| 1 week (catch-up) | ~2 min | ~40–60 sec | ~3 min |

The parquet I/O cost (reading the full dataset + writing it back) is
roughly constant regardless of how many new hours are added — it reflects
the size of the existing dataset (~1–2 GB at 5 years). For frequent
updates, consider switching to DuckDB or appending directly to a database
rather than rewriting the full Parquet each run.

### 3. Quick smoke test

```r
source("test_last_week.R")
```

Downloads and processes only the last 7 days. Useful for verifying the
pipeline is working without waiting for the full build. Output goes to
`data/test_last_week.parquet`.

---

## Configuration

All constants are in `R/config.R`:

```r
DATA_DIR     <- "data"
OUTPUT_FILE  <- "data/hail_mesh_60min.parquet"
LOG_FILE     <- "data/build_log.csv"
START_DATE   <- as.Date("2021-01-01")
N_WORKERS    <- 6L          # parallel workers; tune to your core count
RETRY_MAX    <- 3L          # HTTP retry attempts per file
MESH_MIN     <- 0           # only keep mesh_mm > this value
MESH_MISSING <- c(-999, -99, -1, -3)  # MRMS sentinel values to exclude
BASE_URL     <- "https://noaa-mrms-pds.s3.amazonaws.com/CONUS/MESH_Max_60min_00.50"
```

**Tuning `N_WORKERS`:** The bottleneck is `terra::values()` reading a
98 MB uncompressed raster into memory. Workers compete for memory bandwidth,
so more is not always faster. Benchmarks on a 12-core machine:

| Workers | sec/file | Notes |
|---------|----------|-------|
| 2 | 1.14 | Under-utilized |
| 4 | 1.02 | Good baseline |
| 6 | 0.93 | Sweet spot |
| 12 | ~0.93 | Memory bandwidth saturated |

---

## Build Log

`data/build_log.csv` tracks every processed timestamp:

| Column | Description |
|--------|-------------|
| `valid_time` | UTC datetime |
| `status` | `ok`, `missing`, or `error` |
| `nrows` | Hail pixels extracted (0 is valid — no hail that hour) |
| `size_kb` | Downloaded file size in KB |

**`missing`** means the S3 file did not exist (HTTP 404). MRMS has
legitimate gaps — the product was not generated for that hour. These are
retried automatically by `update_dataset.R`.

**`error`** means the file existed but could not be downloaded or parsed
after `RETRY_MAX` attempts. Also retried on next update run.

---

## SQL Integration

Each run of `update_dataset.R` produces a timestamped delta Parquet in
`data/delta/` containing only the rows added in that run.

### DuckDB

```sql
-- Create table
CREATE TABLE hail_mesh (
    valid_time TIMESTAMPTZ NOT NULL,
    lon        DOUBLE      NOT NULL,
    lat        DOUBLE      NOT NULL,
    mesh_mm    DOUBLE      NOT NULL
);

-- Load initial full dataset
INSERT INTO hail_mesh
SELECT * FROM read_parquet('data/hail_mesh_60min.parquet');

-- Load a delta after each update run
INSERT INTO hail_mesh
SELECT * FROM read_parquet('data/delta/hail_mesh_delta_20260319_120000.parquet');

-- Or load all pending deltas at once
INSERT INTO hail_mesh
SELECT * FROM read_parquet('data/delta/*.parquet');
```

### R + DBI

```r
library(DBI)
library(duckdb)
library(arrow)

con <- dbConnect(duckdb::duckdb(), "hail.duckdb")

# Initial load
full <- arrow::read_parquet("data/hail_mesh_60min.parquet")
DBI::dbWriteTable(con, "hail_mesh", full)

# After each update: load the latest delta
delta_files <- sort(fs::dir_ls("data/delta", glob = "*.parquet"))
latest      <- arrow::read_parquet(tail(delta_files, 1))
DBI::dbAppendTable(con, "hail_mesh", latest)
```

### Recommended indexes

```sql
-- For time-range queries
CREATE INDEX idx_valid_time ON hail_mesh (valid_time);

-- For spatial bounding box queries
CREATE INDEX idx_lat_lon ON hail_mesh (lat, lon);

-- For combined time + space queries
CREATE INDEX idx_time_space ON hail_mesh (valid_time, lat, lon);
```

**Tip:** After loading the full 5-year dataset, add a `UNIQUE` constraint
on `(valid_time, lon, lat)` to prevent duplicate rows if delta files are
accidentally loaded twice.

---

## How the Parser Works

Each grib2.gz file is processed as follows:

1. **Download** — `httr2::req_perform()` fetches the ~50 KB file via HTTP
   with up to 3 retries and exponential backoff
2. **Open** — `terra::rast("/vsigzip/<path>")` opens the file via GDAL's
   virtual gzip filesystem (no disk decompression needed)
3. **Extract values** — `terra::values(r, mat=FALSE)` reads all 24.5M
   cell values into a numeric vector
4. **Filter** — `which()` finds the indices of non-zero, non-sentinel cells
   (typically 0.01–0.2% of the grid)
5. **Coordinates** — `terra::xyFromCell(r, idx)` computes lon/lat only for
   the hail cells, avoiding 24.5M coordinate allocations
6. **Write** — worker writes a per-hour Parquet to a temp directory;
   only a small metadata list crosses the IPC boundary back to the main
   process (avoids furrr socket serialization of large tibbles)

Steps 4–5 are the key optimization: a naive `as.data.frame(r, xy=TRUE)`
materializes all 24.5M rows before filtering, taking ~4 sec/file. The
`values()+which()+xyFromCell()` approach takes ~0.85 sec/file (**5x faster**).

---

## Known Limitations

- **CONUS only.** Alaska, Hawaii, Guam, and Caribbean products exist on
  S3 under different prefixes but are not included.
- **Top-of-hour sampling.** The MESHMax60min product updates every 2 minutes
  operationally. This project samples only the `HH:00:00` file. Hail events
  that peak between hours are captured by the 60-min rolling max, but the
  exact peak minute is not preserved.
- **Archive starts 2021-01-01.** S3 data exists from October 2020 but
  `START_DATE` is set to 2021-01-01 for a clean calendar year boundary.
  Change `START_DATE` in `R/config.R` to extend back further.
- **MESH is a model estimate**, not ground truth. It is calibrated against
  historical hail reports but can overestimate in some environments and
  underestimate in others.

---

## References

- [MRMS Product Table](https://www.nssl.noaa.gov/projects/mrms/operational/tables.php)
- [MRMS on AWS Open Data](https://registry.opendata.aws/noaa-mrms-pds/)
- [MESH Algorithm (Witt et al. 1998)](https://doi.org/10.1175/1520-0434(1998)013<0286:AEHDUS>2.0.CO;2)
