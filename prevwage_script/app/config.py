import os

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
MONDAY_API_URL = "https://api.monday.com/v2"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

SAM_BASE_URL = "https://sam.gov/wage-determinations"
DEFAULT_CONSTRUCTION_TYPE = "building"

REQUESTS_BOARD_ID = 18402509426
RESULTS_BOARD_ID = 18402789789

REQ_COL_CITY_STATE_ZIP = "text_mm15g654"
REQ_COL_INSTALLER = "person"
REQ_COL_STATUS = "status"
REQ_COL_DATE_NEEDED = "date4"
REQ_COL_NOTES = "text_mm14a2aj"

RES_COL_PERSON = "person"
RES_COL_STATUS = "status"
RES_COL_EFFECTIVE_DATE = "date4"
RES_COL_CITY_STATE_ZIP = "text_mm19tmfc"
RES_COL_COUNTY = "text_mm19787k"
RES_COL_FIPS = "text_mm192ady"
RES_COL_BASE_RATE = "numeric_mm19nxsc"
RES_COL_FRINGE_RATE = "numeric_mm19b9pk"

REQ_STATUS_LOOKUP_QUEUED = "Lookup Queued"
REQ_STATUS_RATE_FOUND = "Rate Found"
REQ_STATUS_REVIEWED = "Reviewed"
REQ_STATUS_FAILED = "Failed"

RES_STATUS_DONE = "Done"