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

What learning needed from the same embeddings stayed behind at the time,
coordinate-free: the **semantic partition** and the **semantic regions** built
on it. Both existed for the Signal Lab, and both left with it under D25 — see
[Signal Lab](signal-lab.md). Their tables are left in place, unread.

Old links to `#/map` land on Discovery.
