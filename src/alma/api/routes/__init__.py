"""API route modules."""

from .activity import router as activity_router
from .ai import router as ai_router
from .alerts import router as alerts_router
from .authors import router as authors_router
from .discovery import router as discovery_router
from .feed import router as feed_router
from .graphs import router as graphs_router
from .imports import router as imports_router
from .insights import router as insights_router
from .library import router as library_router
from .library_mgmt import router as library_mgmt_router
from .lenses import router as lenses_router
from .logs import router as logs_router
from .operations import router as operations_router
from .plugins import router as plugins_router
from .publications import router as papers_router
from .scheduler import router as scheduler_router
from .settings import router as settings_router
from .tags import router as tags_router

__all__ = [
    "activity_router",
    "ai_router",
    "alerts_router",
    "authors_router",
    "discovery_router",
    "feed_router",
    "graphs_router",
    "imports_router",
    "insights_router",
    "library_router",
    "library_mgmt_router",
    "lenses_router",
    "logs_router",
    "operations_router",
    "plugins_router",
    "papers_router",
    "scheduler_router",
    "settings_router",
    "tags_router",
]
