# R/config.R -- Project constants
# ============================================================================

DATA_DIR     <- "data"
CHUNK_DIR    <- file.path(DATA_DIR, "chunks")
OUTPUT_FILE  <- file.path(DATA_DIR, "hail_mesh_60min.parquet")
LOG_FILE     <- file.path(DATA_DIR, "build_log.csv")

START_DATE   <- as.Date("2021-01-01")
N_WORKERS    <- 4L
RETRY_MAX    <- 3L
MESH_MIN     <- 0        # keep only mesh_mm > MESH_MIN
MESH_MISSING <- c(-999, -99, -1, -3)  # MRMS sentinel values to exclude

BASE_URL <- "https://noaa-mrms-pds.s3.amazonaws.com/CONUS/MESH_Max_60min_00.50"
