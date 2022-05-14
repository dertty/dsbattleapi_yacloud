from fastapi import FastAPI

from api.api_v1.api import router as api_v1_router
from api.api_v2.api import router as api_v2_router
from core.config import API_V1_STR, API_V2_STR, PROJECT_NAME

from api_versioning import VersionedAPI


app = FastAPI(
    title=PROJECT_NAME,
    description="This is API for the DSBattle platform",
)

app.include_router(api_v1_router, prefix=API_V1_STR)
app.include_router(api_v2_router, prefix=API_V2_STR)

app = VersionedAPI(app)