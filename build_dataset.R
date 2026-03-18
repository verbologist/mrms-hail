#!/usr/bin/env Rscript
# build_dataset.R -- Build complete MRMS MESH 60-min hail dataset (2021 to present)
#
# Usage: Rscript build_dataset.R
#        source("build_dataset.R")  # from RStudio
#
# Output:
#   data/hail_mesh_60min.parquet  -- full dataset, non-zero hail pixels only
#   data/build_log.csv            -- per-hour status log (enables resume)
#   data/chunks/mesh_YYYY-MM.parquet -- intermediate monthly chunks
#
# Resume: safe to interrupt and re-run; completed months are skipped.
# ============================================================================

source("R/config.R")
source("R/utils.R")

# ----------------------------------------------------------------------------
# 0. Setup
# ----------------------------------------------------------------------------

ensure_dirs()
future::plan(future::multisession, workers = N_WORKERS)
on.exit(future::plan(future::sequential), add = TRUE)

t0 <- Sys.time()
cli::cli_h1("MRMS MESH 60-min Dataset Builder")

# ----------------------------------------------------------------------------
# 1. Define full timestamp range
# ----------------------------------------------------------------------------

end_time  <- lubridate::floor_date(lubridate::now("UTC"), "hour") - lubridate::hours(1)
all_times <- generate_hourly_seq(START_DATE, end_time)

cli::cli_alert_info("Date range: {START_DATE} to {format(end_time, '%Y-%m-%d %H:%M UTC')}")
cli::cli_alert_info("Total expected timestamps: {length(all_times)}")

# ----------------------------------------------------------------------------
# 2. Determine remaining work (resume support)
# ----------------------------------------------------------------------------

log_existing <- read_build_log()
completed    <- log_existing |>
  dplyr::filter(status == "ok") |>
  dplyr::pull(valid_time)

remaining <- all_times[!all_times %in% completed]

cli::cli_alert_info("Already completed: {length(completed)}")
cli::cli_alert_info("Remaining: {length(remaining)}")

if (length(remaining) == 0) {
  cli::cli_alert_success("Build already complete.")

  # Still combine chunks if final output doesn't exist yet
  if (!fs::file_exists(OUTPUT_FILE)) {
    cli::cli_alert_info("Final parquet missing -- combining chunks...")
    chunk_files <- fs::dir_ls(CHUNK_DIR, glob = "*.parquet")
    if (length(chunk_files) > 0) {
      all_data <- purrr::map_dfr(chunk_files, arrow::read_parquet) |>
        dplyr::arrange(valid_time, lat, lon)
      arrow::write_parquet(all_data, OUTPUT_FILE)
      cli::cli_alert_success("Written: {OUTPUT_FILE} ({nrow(all_data)} rows)")
    }
  }

  quit(save = "no")
}

# ----------------------------------------------------------------------------
# 3. Split remaining into monthly chunks
# ----------------------------------------------------------------------------

remaining_df <- tibble::tibble(valid_time = remaining) |>
  dplyr::mutate(month_key = format(valid_time, "%Y-%m", tz = "UTC"))

month_groups <- split(remaining_df$valid_time, remaining_df$month_key)
month_groups <- month_groups[order(names(month_groups))]  # chronological

cli::cli_alert_info("Monthly chunks to process: {length(month_groups)}")

# ----------------------------------------------------------------------------
# 4. Process each month
# ----------------------------------------------------------------------------

for (month_key in names(month_groups)) {
  timestamps <- month_groups[[month_key]]
  chunk_file <- file.path(CHUNK_DIR, paste0("mesh_", month_key, ".parquet"))

  result <- process_chunk(timestamps, month_key)

  # Write chunk parquet (write even if 0 rows so we know month was processed)
  if (nrow(result$data) > 0) {
    arrow::write_parquet(result$data, chunk_file)
  } else {
    # Write an empty parquet with correct schema
    arrow::write_parquet(empty_mesh_tibble(), chunk_file)
  }

  # Persist log after every chunk (this is the resume checkpoint)
  append_build_log(result$log)

  # Stats
  n_ok      <- sum(result$log$status == "ok")
  n_missing <- sum(result$log$status == "missing")
  n_error   <- sum(result$log$status == "error")
  n_rows    <- nrow(result$data)

  cli::cli_alert(
    "{month_key}: {n_rows} hail rows | ok={n_ok} missing={n_missing} error={n_error}"
  )
}

# ----------------------------------------------------------------------------
# 5. Combine all chunks into final output
# ----------------------------------------------------------------------------

cli::cli_h2("Combining chunks into final parquet")

chunk_files <- fs::dir_ls(CHUNK_DIR, glob = "*.parquet")
cli::cli_alert_info("Reading {length(chunk_files)} chunk files...")

all_data <- purrr::map_dfr(chunk_files, arrow::read_parquet) |>
  dplyr::filter(mesh_mm > MESH_MIN) |>  # extra safety filter
  dplyr::arrange(valid_time, lat, lon)

arrow::write_parquet(all_data, OUTPUT_FILE)

# ----------------------------------------------------------------------------
# 6. Summary
# ----------------------------------------------------------------------------

final_log <- read_build_log()
elapsed   <- round(difftime(Sys.time(), t0, units = "hours"), 2)

cli::cli_h1("Build Complete")
cli::cli_alert_success("Output: {OUTPUT_FILE}")
cli::cli_alert_success("Total hail rows: {nrow(all_data)}")
cli::cli_alert_success("Hours processed: {nrow(final_log)}")
cli::cli_alert_warning(
  "Missing (no file): {sum(final_log$status == 'missing')}"
)
cli::cli_alert_danger(
  "Errors: {sum(final_log$status == 'error')}"
)
cli::cli_alert_info("Elapsed: {elapsed} hours")
cli::cli_alert_info(
  "Date range in data: {min(all_data$valid_time)} to {max(all_data$valid_time)}"
)
