# Maps

The semantic map — the 2-D layout of your corpus, the preference terrain drawn
over it, the author map, and region selection — is **not part of this line of
ALMa**. ALMa's core is Feed, Discovery and Library (product decision D24); the
map was extracted so the core carries no layout compute, no map routes and no
map jobs.

Nothing was lost:

- The complete map implementation is preserved on the git branch
  `feature/maps-plugin`.
- The database artifacts it built (`publication_clusters` and the stored graph
  views) are left in place; nothing reads them here, and nothing deletes them.

What learning needed from the same embeddings stayed, coordinate-free:

- the **semantic partition** (`application/semantic_partition.py`) — clusters of
  papers by what they are about, with no coordinates;
- the **semantic regions** built on it (`semantic:regions`), which Signal Lab
  samples from and scores against.

Both are refreshed by one background operation, `semantic.partition.refresh`
(a periodic tick and the Health repair *learning_partition* share it), visible
in Activity.

Old links to `#/map` land on Discovery.
