"""Auto tagger: suggests tags for papers from the ones you already use.

This file is the manifest and stays light. Everything heavy sits behind the lazy
callables below, because core reaches this module while it is still importing.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from alma.addons.auto_tagger.schema import DDL, TABLES, has_user_data, move_to_merged_tag
from alma.core.addons import AddOn
from alma.core.job_policy import JobClass, JobPolicy, ResourceKind

_JOB_POLICIES = (
    JobPolicy(
        namespace="tags",
        job_class=JobClass.USER_PRODUCT,
        priority=10,
        resources=frozenset({ResourceKind.DB_WRITER}),
        max_concurrency=2,
    ),
)


def _routers() -> Sequence[tuple[Any, str, Sequence[str]]]:
    from alma.addons.auto_tagger.routes import router

    return ((router, "/api/v1/tags/suggestions", ("tags",)),)


ADDON = AddOn(
    id="auto_tagger",
    name="Auto tagger",
    description="Suggests tags for a paper from similar tagged papers. You accept or dismiss each one.",
    docs_path="concepts/library/",
    has_user_data=has_user_data,
    routers=_routers,
    job_policies=lambda: _JOB_POLICIES,
    ddl=DDL,
    tables=TABLES,
    paper_tables=(("tag_suggestions", True),),
    operation_prefixes=("tags",),
    on_tag_merged=move_to_merged_tag,
)
