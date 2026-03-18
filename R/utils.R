# R/utils.R -- Helper functions for MRMS MESH 60-min processing
# ============================================================================

suppressPackageStartupMessages({
  library(httr2)
  library(terra)
  library(arrow)
  library(dplyr)
  library(lubridate)
  library(fs)
  library(cli)
  library(readr)
  library(purrr)
  library(tibble)
  library(furrr)
})

# ----------------------------------------------------------------------------
# Filesystem setup
# ----------------------------------------------------------------------------

ensure_dirs <- function() {
  fs::dir_create(DATA_DIR)
  fs::dir_create(CHUNK_DIR)
  invisible(NULL)
}

# ----------------------------------------------------------------------------
# URL construction
# ----------------------------------------------------------------------------

make_url <- function(valid_time) {
  # Constructs the S3 HTTP URL for a given top-of-hour POSIXct timestamp.
  # Pattern: {BASE_URL}/{YYYYMMDD}/MRMS_MESH_Max_60min_00.50_{YYYYMMDD}-{HH}0000.grib2.gz
  date_str <- format(valid_time, "%Y%m%d", tz = "UTC")
  hour_str <- format(valid_time, "%H",     tz = "UTC")
  fname    <- sprintf("MRMS_MESH_Max_60min_00.50_%s-%s0000.grib2.gz", date_str, hour_str)
  paste(BASE_URL, date_str, fname, sep = "/")
}

# ----------------------------------------------------------------------------
# Download
# ----------------------------------------------------------------------------

download_grib <- function(url, dest, retries = RETRY_MAX) {
  # Downloads a .grib2.gz from S3 via HTTP.
  # Returns: TRUE (success), FALSE (404 / not found), NA (error after retries)

  for (attempt in seq_len(retries)) {
    result <- tryCatch({
      resp <- request(url) |>
        req_timeout(60) |>
        req_error(is_error = \(resp) FALSE) |>
        req_perform(path = dest)

      status <- resp_status(resp)

      if (status == 200L) {
        return(TRUE)
      } else if (status == 404L) {
        if (fs::file_exists(dest)) fs::file_delete(dest)
        return(FALSE)
      } else {
        # Other HTTP error — may be transient
        if (fs::file_exists(dest)) fs::file_delete(dest)
        cli::cli_warn("HTTP {status} on attempt {attempt} for {basename(url)}")
        NULL  # trigger retry
      }
    }, error = function(e) {
      cli::cli_warn("Download error attempt {attempt}: {e$message}")
      NULL
    })

    if (!is.null(result)) return(result)
    if (attempt < retries) Sys.sleep(2^attempt)
  }

  if (fs::file_exists(dest)) fs::file_delete(dest)
  return(NA)
}

# ----------------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------------

empty_mesh_tibble <- function() {
  tibble::tibble(
    valid_time = as.POSIXct(character(0), tz = "UTC"),
    lon        = double(0),
    lat        = double(0),
    mesh_mm    = double(0)
  )
}

parse_grib <- function(gz_path, valid_time) {
  # Reads a .grib2.gz file, returns tibble of non-zero hail pixels.
  # Tries GDAL /vsigzip/ first; falls back to manual gunzip on Windows.

  r <- tryCatch(terra::rast(gz_path), error = function(e) NULL)

  # Fallback: manually decompress then read
  # NOTE: terra is lazy — rast() only opens the file; values are read during
  # as.data.frame(). Do NOT delete the decompressed file until after extraction.
  grib_path <- NULL
  if (is.null(r)) {
    grib_path <- sub("\\.gz$", "", gz_path)
    R.utils::gunzip(gz_path, destname = grib_path, overwrite = TRUE, remove = FALSE)
    r <- tryCatch(terra::rast(grib_path), error = function(e) NULL)
  }

  if (is.null(r)) {
    if (!is.null(grib_path) && fs::file_exists(grib_path)) fs::file_delete(grib_path)
    cli::cli_warn("Could not read grib2 for {valid_time}")
    return(empty_mesh_tibble())
  }

  df <- terra::as.data.frame(r, xy = TRUE)

  # Safe to delete decompressed file now that data is in memory
  if (!is.null(grib_path) && fs::file_exists(grib_path)) fs::file_delete(grib_path)

  # Positional rename: x, y, <layer_name> -> lon, lat, mesh_mm
  names(df) <- c("lon", "lat", "mesh_mm")

  df <- df |>
    dplyr::filter(
      !is.na(mesh_mm),
      !(mesh_mm %in% MESH_MISSING),
      mesh_mm > MESH_MIN
    ) |>
    dplyr::mutate(valid_time = valid_time) |>
    dplyr::select(valid_time, lon, lat, mesh_mm) |>
    tibble::as_tibble()

  df
}

# ----------------------------------------------------------------------------
# Per-hour processing
# ----------------------------------------------------------------------------

process_one_hour <- function(valid_time, temp_dir = tempdir()) {
  # End-to-end: download one file, parse, return results + metadata.
  # Returns: list($data, $status, $nrows, $size_kb)

  url   <- make_url(valid_time)
  fname <- basename(url)
  dest  <- file.path(temp_dir, fname)

  dl <- download_grib(url, dest)

  if (isFALSE(dl)) {
    return(list(
      data    = empty_mesh_tibble(),
      status  = "missing",
      nrows   = 0L,
      size_kb = NA_real_
    ))
  }

  if (is.na(dl)) {
    return(list(
      data    = empty_mesh_tibble(),
      status  = "error",
      nrows   = 0L,
      size_kb = NA_real_
    ))
  }

  size_kb <- as.numeric(fs::file_size(dest)) / 1024

  df <- tryCatch(
    parse_grib(dest, valid_time),
    error = function(e) {
      cli::cli_warn("Parse failed {valid_time}: {e$message}")
      empty_mesh_tibble()
    }
  )

  if (fs::file_exists(dest)) fs::file_delete(dest)

  list(
    data    = df,
    status  = "ok",
    nrows   = nrow(df),
    size_kb = size_kb
  )
}

# ----------------------------------------------------------------------------
# Timestamp sequences
# ----------------------------------------------------------------------------

generate_hourly_seq <- function(start_date, end_time) {
  # Returns POSIXct vector of top-of-hour UTC timestamps.
  start_time <- as.POSIXct(paste(start_date, "00:00:00"), tz = "UTC")
  end_floor  <- lubridate::floor_date(end_time, "hour")
  seq(start_time, end_floor, by = "1 hour")
}

# ----------------------------------------------------------------------------
# Build log
# ----------------------------------------------------------------------------

read_build_log <- function() {
  if (!fs::file_exists(LOG_FILE)) {
    return(tibble::tibble(
      valid_time = as.POSIXct(character(0), tz = "UTC"),
      status     = character(0),
      nrows      = integer(0),
      size_kb    = double(0)
    ))
  }
  readr::read_csv(
    LOG_FILE,
    col_types = readr::cols(
      valid_time = readr::col_datetime(format = ""),
      status     = readr::col_character(),
      nrows      = readr::col_integer(),
      size_kb    = readr::col_double()
    ),
    show_col_types = FALSE
  ) |>
    dplyr::mutate(valid_time = lubridate::with_tz(valid_time, "UTC"))
}

append_build_log <- function(new_entries) {
  # Appends new_entries to build_log.csv, deduplicating by valid_time.
  existing <- read_build_log()
  combined <- dplyr::bind_rows(existing, new_entries) |>
    dplyr::arrange(valid_time) |>
    dplyr::distinct(valid_time, .keep_all = TRUE)
  readr::write_csv(combined, LOG_FILE)
}

# ----------------------------------------------------------------------------
# Chunk processing (parallel)
# ----------------------------------------------------------------------------

process_chunk <- function(timestamps, chunk_label) {
  # Parallel-processes a vector of timestamps.
  # Returns: list($data = tibble, $log = tibble)

  cli::cli_alert_info("Processing chunk: {chunk_label} ({length(timestamps)} hours)")

  results <- furrr::future_map(
    timestamps,
    process_one_hour,
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  data_df <- purrr::map_dfr(results, "data")

  log_df <- tibble::tibble(
    valid_time = timestamps,
    status     = purrr::map_chr(results, "status"),
    nrows      = purrr::map_int(results, "nrows"),
    size_kb    = purrr::map_dbl(results, "size_kb")
  )

  list(data = data_df, log = log_df)
}
