from fastapi import APIRouter
from .endpoints.functions import router as api_v2_router


router = APIRouter()
router.include_router(api_v2_router)
