"""Shared embedding-computation worker used by API and scheduler flows."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Callable

from alma.ai.embedding_sources import source_for_provider_name

logger = logging.getLogger(__name__)

FETCH_SOURCE_SEMANTIC_SCHOLAR = "semantic_scholar"


def run_embedding_computation(
    job_id: str,
    *,
    scope: str = "missing_stale",
    set_job_status: Callable[..., None],
    add_job_log: Callable[..., None],
    is_cancellation_requested: Callable[[str], bool],
) -> None:
    """Compute embeddings for missing/stale publication vectors."""
    from alma.ai.environment import get_dependency_environment
    from alma.ai.providers import get_active_provider
    from alma.api.deps import open_db_connection
    from alma.discovery.similarity import prepare_text, prepare_text_specter2

    conn = open_db_connection()

    try:
        provider = get_active_provider(conn)
        if provider is None:
            set_job_status(
                job_id,
                status="failed",
                message="No active embedding provider available",
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        dep_env = get_dependency_environment(conn)
        add_job_log(
            job_id,
            "Dependency environment resolved",
            step="environment",
            data={
                "configured_type": dep_env.configured_type,
                "configured_path": dep_env.configured_path,
                "valid": dep_env.valid,
                "effective_python_executable": dep_env.effective_python_executable,
                "effective_python_version": dep_env.effective_python_version,
                "selected_python_executable": dep_env.selected_python_executable,
            },
        )

        provider_runtime = {"provider": provider.name, "model": provider.model_name}
        try:
            if provider.name == "local":
                model_config = getattr(provider, "model_config", None)
                from alma.discovery.similarity import SpecterEmbedder

                embedder = SpecterEmbedder.get_instance(
                    model_name=provider.model_name,
                    max_length=getattr(model_config, "max_tokens", 512),
                )
                provider_runtime["device"] = embedder.device
            add_job_log(
                job_id,
                f"Embedding provider ready: {provider.name}"
                + (f" (device={provider_runtime.get('device')})" if provider_runtime.get("device") else ""),
                step="preflight",
                data=provider_runtime,
            )
        except Exception:
            pass

        try:
            probe = provider.embed(["embedding preflight"])
            if not probe or not probe[0]:
                raise RuntimeError("Provider returned empty embedding in preflight")
        except Exception as exc:
            raw_msg = str(exc)
            user_msg = f"Embedding provider preflight failed: {raw_msg}"
            if "numpy.dtype size changed" in raw_msg:
                user_msg = (
                    "Embedding provider preflight failed due to binary dependency mismatch "
                    "(numpy/scipy/sklearn/torch were built against incompatible versions). "
                    "Reinstall these packages in the SAME selected dependency environment and retry."
                )
            elif "Local SPECTER2 requires" in raw_msg:
                user_msg = raw_msg
            add_job_log(
                job_id,
                user_msg,
                level="ERROR",
                step="preflight_error",
                data={
                    "raw_error": raw_msg,
                    "provider": provider.name,
                    "effective_python_executable": dep_env.effective_python_executable,
                    "effective_python_version": dep_env.effective_python_version,
                },
            )
            set_job_status(
                job_id,
                status="failed",
                message=user_msg,
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        set_job_status(
            job_id,
            status="running",
            message=f"Computing embeddings with {provider.name} ({scope})",
        )

        model_config = getattr(provider, "model_config", None)
        max_tokens = model_config.max_tokens if model_config else 256
        model_hf_id = provider.model_name
        is_local_specter2 = provider.name == "local" and model_hf_id == "allenai/specter2_base"

        if scope == "missing":
            # "Missing" means "no row for the active model", not "no row at all".
            # With the (paper_id, model) PK the paper may have a vector from
            # a previous model yet still need one for the current model.
            rows = conn.execute(
                """
                SELECT p.id, p.title, p.abstract
                FROM papers p
                LEFT JOIN publication_embeddings pe
                  ON pe.paper_id = p.id AND pe.model = ?
                WHERE pe.paper_id IS NULL
                """,
                (model_hf_id,),
            ).fetchall()
        elif scope == "stale":
            # A paper is "stale" for the active model if it has vectors under
            # some other model but no vector under the active one.
            rows = conn.execute(
                """
                SELECT p.id, p.title, p.abstract
                FROM papers p
                WHERE EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model <> ?
                )
                AND NOT EXISTS (
                    SELECT 1 FROM publication_embeddings pe
                    WHERE pe.paper_id = p.id AND pe.model = ?
                )
                """,
                (model_hf_id, model_hf_id),
            ).fetchall()
        elif scope == "all":
            rows = conn.execute(
                """
                SELECT p.id, p.title, p.abstract
                FROM papers p
                """,
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT p.id, p.title, p.abstract
                FROM papers p
                LEFT JOIN publication_embeddings pe
                  ON pe.paper_id = p.id AND pe.model = ?
                WHERE pe.paper_id IS NULL
                """,
                (model_hf_id,),
            ).fetchall()

        total = len(rows)
        if total == 0:
            set_job_status(
                job_id,
                status="completed",
                message=f"No papers matched embedding scope '{scope}'",
                processed=0,
                total=0,
                finished_at=datetime.utcnow().isoformat(),
            )
            return

        if is_local_specter2:
            try:
                local_fill_rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS c
                    FROM publication_embedding_fetch_status
                    WHERE model = ?
                      AND source = ?
                      AND status IN ('unmatched', 'missing_vector')
                    GROUP BY status
                    """,
                    (model_hf_id, FETCH_SOURCE_SEMANTIC_SCHOLAR),
                ).fetchall()
                local_fill_counts = {str(row["status"]): int(row["c"] or 0) for row in local_fill_rows}
            except sqlite3.OperationalError:
                local_fill_counts = {}
            add_job_log(
                job_id,
                "Local SPECTER2 will compute cached vectors for papers still missing after S2 fetch",
                step="local_specter2_fill",
                data={
                    "candidate_papers": total,
                    "s2_terminal_misses": local_fill_counts,
                    "manual_user_action": True,
                },
            )

        processed = 0
        errors = 0
        skipped_empty = 0
        error_types: dict[str, int] = {}
        error_details_logged = 0
        batch_size = 16

        def _record_error(row: sqlite3.Row, exc: Exception, step: str = "embed_error") -> None:
            nonlocal errors, error_details_logged
            errors += 1
            err_name = exc.__class__.__name__
            error_types[err_name] = error_types.get(err_name, 0) + 1
            logger.warning(
                "Failed to embed paper %s: %s",
                row["id"],
                exc,
            )
            if error_details_logged < 20:
                add_job_log(
                    job_id,
                    f"{err_name} for paper {row['id']}: {exc}",
                    level="ERROR",
                    step=step,
                    data={
                        "paper_id": row["id"],
                        "title": row["title"],
                    },
                )
                error_details_logged += 1

        i = 0
        while i < total:
            if is_cancellation_requested(job_id):
                add_job_log(job_id, "Cancellation requested; stopping embedding loop", step="cancelled")
                set_job_status(
                    job_id,
                    status="cancelled",
                    processed=processed,
                    total=total,
                    errors=errors,
                    message="Embedding computation cancelled by user",
                    finished_at=datetime.utcnow().isoformat(),
                    result={
                        "processed": processed,
                        "total": total,
                        "errors": errors,
                        "skipped_empty": skipped_empty,
                        "cancelled": True,
                    },
                )
                return

            chunk = rows[i:i + batch_size]
            payload: list[tuple[sqlite3.Row, str]] = []
            for row in chunk:
                topics: list[str] = []
                try:
                    topic_rows = conn.execute(
                        "SELECT term FROM publication_topics WHERE paper_id = ?",
                        (row["id"],),
                    ).fetchall()
                    topics = [
                        r["term"] if isinstance(r, sqlite3.Row) else r[0]
                        for r in topic_rows
                        if (r["term"] if isinstance(r, sqlite3.Row) else r[0])
                    ]
                except sqlite3.OperationalError:
                    pass

                if is_local_specter2:
                    # SPECTER2 expects canonical `title + [SEP] + abstract`
                    # (per the HF model card). Using `prepare_text`'s
                    # enriched format pushes vectors ~0.99 cosine off the
                    # S2-downloaded ones; the canonical input lifts the
                    # match to ~1.0 on the median paper.
                    text = prepare_text_specter2(row["title"] or "", row["abstract"] or "")
                else:
                    text = prepare_text(
                        row["title"] or "",
                        row["abstract"] or "",
                        topics=topics,
                        max_tokens=max_tokens,
                        is_query=False,
                    )
                if not text:
                    skipped_empty += 1
                    processed += 1
                    continue
                payload.append((row, text))

            if payload:
                try:
                    import numpy as np

                    embeddings = provider.embed([p[1] for p in payload])
                    if len(embeddings) != len(payload):
                        raise RuntimeError(
                            f"Provider returned {len(embeddings)} embeddings for {len(payload)} inputs"
                        )

                    inserted_paper_ids: list[str] = []
                    for (row, _text), emb in zip(payload, embeddings):
                        embedding_array = np.array(emb, dtype=np.float32)
                        conn.execute(
                            "INSERT OR REPLACE INTO publication_embeddings "
                            "(paper_id, embedding, model, source, created_at) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (
                                row["id"],
                                embedding_array.tobytes(),
                                model_hf_id,
                                source_for_provider_name(provider.name),
                                datetime.utcnow().isoformat(),
                            ),
                        )
                        inserted_paper_ids.append(str(row["id"]))
                        processed += 1
                    conn.commit()
                    # Keep `author_centroids` coherent with the new
                    # embeddings — D12 `paper_signal.author_alignment`
                    # reads cached centroids on every score pass.
                    try:
                        from alma.application.author_backfill import (
                            refresh_centroids_for_papers,
                        )

                        refresh_centroids_for_papers(
                            conn, inserted_paper_ids, model=model_hf_id
                        )
                        conn.commit()
                    except Exception:
                        logger.debug(
                            "author centroid refresh skipped after batch insert",
                            exc_info=True,
                        )
                except Exception as batch_exc:
                    add_job_log(
                        job_id,
                        f"Batch embed failed for chunk starting at {i + 1}: {batch_exc}. Falling back to per-item.",
                        level="WARNING",
                        step="batch_fallback",
                    )
                    import numpy as np

                    for row, text in payload:
                        try:
                            emb = provider.embed([text])[0]
                            embedding_array = np.array(emb, dtype=np.float32)
                            conn.execute(
                                "INSERT OR REPLACE INTO publication_embeddings "
                                "(paper_id, embedding, model, source, created_at) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (
                                    row["id"],
                                    embedding_array.tobytes(),
                                    model_hf_id,
                                    source_for_provider_name(provider.name),
                                    datetime.utcnow().isoformat(),
                                ),
                            )
                        except Exception as exc:
                            _record_error(row, exc)
                        finally:
                            processed += 1
                    conn.commit()

            if processed % 25 == 0 or processed == total:
                set_job_status(
                    job_id,
                    status="running",
                    processed=processed,
                    total=total,
                    errors=errors,
                    message=f"Computed {processed}/{total} embeddings",
                )
                add_job_log(
                    job_id,
                    f"Progress {processed}/{total} (errors={errors}, skipped_empty={skipped_empty}, scope={scope})",
                    step="progress",
                    data={"error_types": dict(error_types), "scope": scope},
                )
            i += batch_size

        add_job_log(
            job_id,
            "Embedding computation summary",
            step="summary",
            data={
                "processed": processed,
                "total": total,
                "errors": errors,
                "skipped_empty": skipped_empty,
                "error_types": dict(error_types),
                "scope": scope,
            },
        )
        set_job_status(
            job_id,
            status="completed",
            processed=processed,
            total=total,
            errors=errors,
            message=(
                f"Completed ({scope}): {processed - errors - skipped_empty}/{total} embeddings computed "
                f"({errors} errors, {skipped_empty} empty texts skipped)"
            ),
            finished_at=datetime.utcnow().isoformat(),
        )
    except Exception as exc:
        logger.exception("Embedding computation failed: %s", exc)
        set_job_status(
            job_id,
            status="failed",
            message=f"Embedding computation failed: {exc}",
            finished_at=datetime.utcnow().isoformat(),
        )
    finally:
        conn.close()
