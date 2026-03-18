#!/usr/bin/env Rscript
# test_last_week.R -- Download last 7 days and write test parquet
# ============================================================================

setwd("C:/claudegit/mrms-hail")
source("R/config.R")
source("R/utils.R")

OUTPUT_FILE <- "data/test_last_week.parquet"
LOG_FILE    <- "data/test_last_week_log.csv"

ensure_dirs()
future::plan(future::multisession, workers = N_WORKERS)
on.exit(future::plan(future::sequential), add = TRUE)

t0       <- Sys.time()
end_time <- lubridate::floor_date(lubridate::now("UTC"), "hour") - lubridate::hours(1)
start    <- end_time - lubridate::days(7)
times    <- generate_hourly_seq(as.Date(start), end_time)

cat(sprintf("Testing %d hours: %s to %s\n",
    length(times),
    format(min(times), "%Y-%m-%d %H:%M UTC"),
    format(max(times), "%Y-%m-%d %H:%M UTC")))

# Process in daily chunks so progress is visible
times_df     <- tibble::tibble(valid_time = times) |>
  dplyr::mutate(day_key = format(valid_time, "%Y-%m-%d", tz = "UTC"))
day_groups   <- split(times_df$valid_time, times_df$day_key)
day_groups   <- day_groups[order(names(day_groups))]

all_data <- list()
all_log  <- list()

for (day_key in names(day_groups)) {
  result <- process_chunk(day_groups[[day_key]], day_key)
  all_data[[day_key]] <- result$data
  all_log[[day_key]]  <- result$log

  n_ok      <- sum(result$log$status == "ok")
  n_missing <- sum(result$log$status == "missing")
  cat(sprintf("%s | rows: %d | ok: %d missing: %d\n",
      day_key, nrow(result$data), n_ok, n_missing))
}

final_data <- dplyr::bind_rows(all_data) |>
  dplyr::filter(mesh_mm > MESH_MIN) |>
  dplyr::arrange(valid_time, lat, lon)

final_log <- dplyr::bind_rows(all_log)

arrow::write_parquet(final_data, OUTPUT_FILE)
readr::write_csv(final_log, LOG_FILE)

elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 1)
cat(sprintf("\nDone: %d hail rows across %d hours | %.1f min elapsed\n",
    nrow(final_data), length(times), as.numeric(elapsed)))
cat(sprintf("Output: %s\n", OUTPUT_FILE))
cat(sprintf("Max MESH: %.1f mm\n", max(final_data$mesh_mm, na.rm = TRUE)))
cat(sprintf("Hours with hail: %d / %d\n",
    length(unique(final_data$valid_time)), length(times)))
