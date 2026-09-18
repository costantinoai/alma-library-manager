"""Paper PDFs — the core feature (store, verify, fetch, attach, import, serve).

Contract: :mod:`alma.application.pdf_schema`. Modules, in dependency order:

- ``store``    — the readable file tree, staging, DDL and row helpers (no network).
- ``verify``   — is it a PDF, and is it THIS paper (pypdf; CPU only).
- ``identify`` — which paper a dropped PDF is (identity hints from its bytes).
- ``service``  — the use cases, each the body of one Activity job.
- ``jobs``     — queue a use case as an enveloped Activity job (``pdf.*``).

Routes stay thin and live with their canonical owners: the papers router
(serve / attach / fetch / remove, status in ``/papers/{id}/details``) and the
imports router (PDF-first import).
"""
