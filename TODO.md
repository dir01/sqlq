# TODO

- [x] Recover abandoned jobs in both database drivers with expiring claims,
  ownership checks, conditional extension, and worker-loss recovery tests,
  including prefetched jobs.
- [x] Fix SQLite dead-letter deletion using the supplied job ID and an ownership
  check, verify one row was deleted, and cover committed and rolled-back moves.
