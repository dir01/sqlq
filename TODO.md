# TODO

- [ ] Recover abandoned jobs in both database drivers. Claims currently set
  `consumed_at` permanently, so a worker crash or shutdown after prefetching can
  strand jobs. Add expiring leases with ownership checks and tests for recovery
  after worker loss, including prefetched jobs.
- [ ] Fix SQLite dead-letter deletion. `moveToDeadLetterQueue` deletes using
  `job.ID`, which is never populated, instead of `jobID`. Use the supplied ID,
  verify that one row was deleted, and test that a committed move leaves the job
  only in the DLQ while a rolled-back move preserves the original job.
