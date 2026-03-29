from fastapi import APIRouter
from app.api import health, graphs, papers, search, ingestions

router = APIRouter()
router.include_router(health.router)
router.include_router(graphs.router)
router.include_router(papers.router)
router.include_router(search.router)
router.include_router(ingestions.router)
