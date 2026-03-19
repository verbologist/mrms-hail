# R/install_packages.R -- One-time package setup
# ============================================================================

cran_pkgs <- c(
  "httr2",       # HTTP downloads
  "terra",       # grib2 reading via GDAL
  "arrow",       # Parquet read/write
  "dplyr",       # data wrangling
  "lubridate",   # datetime handling
  "future",      # parallel backend
  "furrr",       # parallel map (future-aware purrr)
  "fs",          # file system utilities
  "cli",         # progress bars and messages
  "readr",       # CSV read/write
  "purrr",       # functional programming
  "tibble",      # tibble construction
  "R.utils",     # gunzip fallback for grib2.gz on Windows
  "tigris",      # US Census county/state boundaries (grid lookup)
  "sf",          # spatial operations (grid lookup)
  "duckdb"       # streaming combine of large chunk parquets (build step)
)

to_install <- cran_pkgs[!vapply(cran_pkgs, requireNamespace, logical(1), quietly = TRUE)]

if (length(to_install) > 0) {
  message("Installing: ", paste(to_install, collapse = ", "))
  install.packages(to_install)
} else {
  message("All packages already installed.")
}
