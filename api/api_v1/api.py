from fastapi import APIRouter
from .endpoints.functions import router as api_v1_router


router = APIRouter()
router.include_router(api_v1_router)
