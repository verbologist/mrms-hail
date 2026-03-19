#!/usr/bin/env Rscript
# build_grid_lookup.R -- Build county lookup table for every MRMS MESH grid cell
#
# Usage: Rscript build_grid_lookup.R
#        source("build_grid_lookup.R")  # from RStudio
#
# Output: data/grid_county_lookup.parquet
#
# Maps every 0.01-degree MRMS grid cell center within CONUS to its county,
# using US Census Bureau TIGER/Line boundaries via the tigris package.
#
# Columns:
#   lon          - longitude (WGS84, -130 to -60)
#   lat          - latitude  (WGS84,  20  to  55)
#   fips         - 5-digit combined FIPS code (e.g. "06037")
#   state_fips   - 2-digit state  FIPS (e.g. "06")
#   county_fips  - 3-digit county FIPS (e.g. "037")
#   state_name   - full state name (e.g. "California")
#   state_abbr   - 2-letter postal abbreviation (e.g. "CA")
#   county_name  - county name without type suffix (e.g. "Los Angeles")
#
# Only cells that intersect a county polygon are included (~15-18M of 24.5M).
# Ocean and outside-CONUS cells are dropped (NA from rasterize).
#
# Run time: ~10-20 minutes (rasterize step dominates).
# Output size: ~150-250 MB Parquet.
# ============================================================================

setwd("C:/claudegit/mrms-hail")
source("R/config.R")

suppressPackageStartupMessages({
  library(terra)
  library(tigris)
  library(sf)
  library(dplyr)
  library(arrow)
  library(fs)
  library(cli)
})

options(tigris_use_cache = TRUE)   # cache Census shapefiles locally

LOOKUP_FILE <- file.path(DATA_DIR, "grid_county_lookup.parquet")
ensure_dirs <- function() fs::dir_create(DATA_DIR)
ensure_dirs()

t0 <- Sys.time()
cli::cli_h1("MRMS Grid County Lookup Builder")

# ----------------------------------------------------------------------------
# 1. Download county and state boundaries from Census Bureau
# ----------------------------------------------------------------------------

cli::cli_alert_info("Downloading county boundaries (tigris)...")
counties_sf <- tigris::counties(cb = TRUE, resolution = "500k", year = 2023,
                                 progress_bar = FALSE) |>
  sf::st_transform(4326)   # ensure WGS84

cli::cli_alert_info("Downloading state boundaries (tigris)...")
states_sf <- tigris::states(cb = TRUE, year = 2023,
                             progress_bar = FALSE) |>
  sf::st_drop_geometry() |>
  dplyr::select(STATEFP, NAME, STUSPS) |>
  dplyr::rename(
    state_fips = STATEFP,
    state_name = NAME,
    state_abbr = STUSPS
  )

cli::cli_alert_success("Got {nrow(counties_sf)} counties across {nrow(states_sf)} states/territories")

# ----------------------------------------------------------------------------
# 2. Prepare county attribute table
#    Assign a numeric index for rasterization (terra needs a numeric field)
# ----------------------------------------------------------------------------

counties_attr <- counties_sf |>
  sf::st_drop_geometry() |>
  dplyr::mutate(idx = dplyr::row_number()) |>
  dplyr::select(
    idx,
    fips        = GEOID,
    state_fips  = STATEFP,
    county_fips = COUNTYFP,
    county_name = NAME
  ) |>
  dplyr::left_join(states_sf, by = "state_fips") |>
  dplyr::select(idx, fips, state_fips, county_fips, state_name, state_abbr, county_name)

cli::cli_alert_info("County attribute table: {nrow(counties_attr)} rows")

# ----------------------------------------------------------------------------
# 3. Build MRMS reference grid (must exactly match the MRMS raster spec)
# ----------------------------------------------------------------------------

cli::cli_alert_info("Creating MRMS reference grid (7000 x 3500 = 24.5M cells)...")

mrms_grid <- terra::rast(
  xmin       = -130,
  xmax       = -60,
  ymin       =  20,
  ymax       =  55,
  resolution =  0.01,
  crs        = "EPSG:4326"
)

cli::cli_alert_success(
  "Grid: {terra::ncol(mrms_grid)} cols x {terra::nrow(mrms_grid)} rows = {terra::ncell(mrms_grid)} cells"
)

# ----------------------------------------------------------------------------
# 4. Rasterize county polygons onto the MRMS grid
#    Each cell gets the numeric idx of the county it falls in.
#    Ocean / outside-CONUS cells get NA.
# ----------------------------------------------------------------------------

cli::cli_alert_info("Rasterizing county polygons onto MRMS grid (this takes ~10-15 min)...")

counties_vect  <- terra::vect(counties_sf) |>
  terra::set.values(counties_attr$idx, field = "idx")

# Re-attach idx to the SpatVector via a clean merge
counties_vect2 <- terra::vect(
  counties_sf |> dplyr::mutate(idx = dplyr::row_number())
)

t_rast <- Sys.time()
grid_idx <- terra::rasterize(counties_vect2, mrms_grid, field = "idx")
cli::cli_alert_success(
  "Rasterize complete in {round(difftime(Sys.time(), t_rast, units='mins'), 1)} min"
)

# ----------------------------------------------------------------------------
# 5. Extract to data frame (drops NA = ocean / outside CONUS)
# ----------------------------------------------------------------------------

cli::cli_alert_info("Extracting land cells to data frame...")

df_raw <- terra::as.data.frame(grid_idx, xy = TRUE, na.rm = TRUE)
names(df_raw) <- c("lon", "lat", "idx")
df_raw$idx <- as.integer(df_raw$idx)

cli::cli_alert_success(
  "{nrow(df_raw)} land cells ({round(nrow(df_raw)/terra::ncell(mrms_grid)*100,1)}% of grid)"
)

# ----------------------------------------------------------------------------
# 6. Join county attributes
# ----------------------------------------------------------------------------

cli::cli_alert_info("Joining county attributes...")

lookup <- dplyr::left_join(df_raw, counties_attr, by = "idx") |>
  dplyr::select(lon, lat, fips, state_fips, county_fips, state_name, state_abbr, county_name) |>
  dplyr::arrange(lat, lon)

# Sanity check
n_unmatched <- sum(is.na(lookup$fips))
if (n_unmatched > 0) {
  cli::cli_warn("{n_unmatched} cells had no county match (border artifacts) — dropping")
  lookup <- dplyr::filter(lookup, !is.na(fips))
}

cli::cli_alert_success("Final lookup: {nrow(lookup)} cells across {length(unique(lookup$fips))} counties")

# ----------------------------------------------------------------------------
# 7. Write Parquet
# ----------------------------------------------------------------------------

cli::cli_alert_info("Writing {LOOKUP_FILE}...")
arrow::write_parquet(lookup, LOOKUP_FILE,
                     compression = "snappy")

size_mb <- round(fs::file_size(LOOKUP_FILE) / 1024^2, 1)
elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 1)

cli::cli_h1("Done")
cli::cli_bullets(c(
  "v" = "Output: {LOOKUP_FILE}",
  "v" = "Rows: {nrow(lookup)}",
  "v" = "Counties: {length(unique(lookup$fips))}",
  "v" = "States/territories: {length(unique(lookup$state_fips))}",
  "v" = "File size: {size_mb} MB",
  "i" = "Elapsed: {elapsed} min"
))

# ----------------------------------------------------------------------------
# Quick usage examples
# ----------------------------------------------------------------------------

cat("\n")
cli::cli_h2("Example queries")

cat("# Join hail data to counties:\n")
cat("# hail <- arrow::read_parquet('data/hail_mesh_60min.parquet')\n")
cat("# lkp  <- arrow::read_parquet('data/grid_county_lookup.parquet')\n")
cat("# dplyr::left_join(hail, lkp, by = c('lon','lat'))\n\n")

cat("# Top hail counties in a date range:\n")
cat("# hail |> dplyr::left_join(lkp, by=c('lon','lat')) |>\n")
cat("#   dplyr::filter(valid_time >= '2024-01-01') |>\n")
cat("#   dplyr::group_by(state_abbr, county_name, fips) |>\n")
cat("#   dplyr::summarise(max_mesh_mm = max(mesh_mm), events = n()) |>\n")
cat("#   dplyr::arrange(dplyr::desc(max_mesh_mm))\n")
