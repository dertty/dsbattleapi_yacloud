from starlette.datastructures import CommaSeparatedStrings
import os

ALLOWED_HOSTS = CommaSeparatedStrings(os.getenv("ALLOWED_HOSTS", ""))
API_V1_STR = "/api/v1"
API_V2_STR = "/api/v2"
PROJECT_NAME = "DSBattleAPI"
