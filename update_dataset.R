#!/usr/bin/env Rscript
# update_dataset.R -- Incrementally update MRMS MESH hail dataset to current
#
# Usage: Rscript update_dataset.R
#        source("update_dataset.R")  # from RStudio
#
# What it does:
#   1. Finds the most recent timestamp in the existing parquet
#   2. Fetches all new top-of-hour files up to the current UTC hour
#   3. Retries any previously "missing" or "error" timestamps (gap fill)
#   4. Appends new rows to the parquet and updates the build log
#
# Prerequisite: build_dataset.R must have been run at least once.
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
cli::cli_h1("MRMS MESH Dataset Updater")

# ----------------------------------------------------------------------------
# 1. Validate existing dataset
# ----------------------------------------------------------------------------

if (!fs::file_exists(OUTPUT_FILE)) {
  cli::cli_abort(
    c(
      "No existing dataset found at: {OUTPUT_FILE}",
      "i" = "Run build_dataset.R first to create the initial dataset."
    )
  )
}

existing_data <- arrow::read_parquet(OUTPUT_FILE)
max_time      <- max(existing_data$valid_time)
current_utc   <- lubridate::floor_date(lubridate::now("UTC"), "hour") - lubridate::hours(1)

cli::cli_alert_info("Existing dataset: {nrow(existing_data)} rows")
cli::cli_alert_info("Last timestamp:   {format(max_time, '%Y-%m-%d %H:%M UTC')}")
cli::cli_alert_info("Current UTC hour: {format(current_utc, '%Y-%m-%d %H:%M UTC')}")

# ----------------------------------------------------------------------------
# 2a. New timestamps since last update
# ----------------------------------------------------------------------------

if (max_time >= current_utc) {
  new_times <- as.POSIXct(character(0), tz = "UTC")
} else {
  next_start <- max_time + 3600  # one hour after last known
  new_times  <- seq(next_start, current_utc, by = "1 hour")
}

cli::cli_alert_info("New timestamps to fetch: {length(new_times)}")

# ----------------------------------------------------------------------------
# 2b. Retry previously failed/missing timestamps (gap fill)
# ----------------------------------------------------------------------------

log_existing <- read_build_log()
retry_times  <- log_existing |>
  dplyr::filter(status %in% c("missing", "error")) |>
  dplyr::pull(valid_time)

# Only retry timestamps within the build range (don't retry future timestamps)
retry_times <- retry_times[retry_times <= current_utc]

cli::cli_alert_info("Gap-fill retries: {length(retry_times)}")

# ----------------------------------------------------------------------------
# 3. Combine and deduplicate
# ----------------------------------------------------------------------------

all_update_times <- sort(unique(c(new_times, retry_times)))

if (length(all_update_times) == 0) {
  cli::cli_alert_success("Dataset is already current. Nothing to do.")
  quit(save = "no")
}

cli::cli_alert_info("Total timestamps to process: {length(all_update_times)}")

# ----------------------------------------------------------------------------
# 4. Process update timestamps
# ----------------------------------------------------------------------------

result <- process_chunk(all_update_times, "update")

# ----------------------------------------------------------------------------
# 5. Build updated dataset
# ----------------------------------------------------------------------------

if (nrow(result$data) > 0) {
  # Remove rows for any retry timestamps from the existing dataset
  # (they may have been partial/wrong; we'll replace with fresh data)
  if (length(retry_times) > 0) {
    existing_data <- existing_data |>
      dplyr::filter(!(valid_time %in% retry_times))
    cli::cli_alert_info("Removed {length(retry_times)} retry-timestamp slots from existing data")
  }

  combined <- dplyr::bind_rows(existing_data, result$data) |>
    dplyr::filter(mesh_mm > MESH_MIN) |>
    dplyr::arrange(valid_time, lat, lon)

  arrow::write_parquet(combined, OUTPUT_FILE)
  new_rows <- nrow(combined) - nrow(existing_data) + length(retry_times)
  cli::cli_alert_success(
    "Parquet updated: {nrow(combined)} total rows (+{nrow(result$data)} new hail pixels)"
  )
} else {
  cli::cli_alert_warning("No new hail data found in update window (all hours had zero pixels or were missing).")
  combined <- existing_data
}

# ----------------------------------------------------------------------------
# 6. Update build log
# ----------------------------------------------------------------------------

append_build_log(result$log)

# ----------------------------------------------------------------------------
# 7. Summary report
# ----------------------------------------------------------------------------

n_ok        <- sum(result$log$status == "ok")
n_missing   <- sum(result$log$status == "missing")
n_error     <- sum(result$log$status == "error")
gaps_filled <- sum(result$log$valid_time %in% retry_times & result$log$status == "ok")
elapsed     <- round(difftime(Sys.time(), t0, units = "mins"), 1)

updated_log <- read_build_log()

cli::cli_h1("Update Summary")
cli::cli_bullets(c(
  "v" = "Hours processed:  {length(all_update_times)}",
  "v" = "New hail rows:    {nrow(result$data)}",
  "v" = "Gaps filled:      {gaps_filled} of {length(retry_times)} retried",
  "!" = "Still missing:    {n_missing}",
  "x" = "Errors:           {n_error}",
  "i" = "Total log entries:{nrow(updated_log)}",
  "i" = "Elapsed:          {elapsed} minutes"
))

cli::cli_alert_info(
  "Dataset now covers: {format(min(combined$valid_time), '%Y-%m-%d')} to {format(max(combined$valid_time), '%Y-%m-%d %H:%M UTC')}"
)
