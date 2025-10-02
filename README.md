# Transaction simulator

- [ ] PlayEvents
    - [ ] rollback
    - [ ] delete
    - [x] read from snapshot
    - [ ] error handling
    - [ ] refactor AFTER ERROR HANDLING
      - [x] extract diagram building
      - [x] extract participant management
      - [ ] strategy of operation instead of switches
      - [ ] Scheduler/Executor which handles concurrency things
        - encapsulates wg, unblocks and "var transactions sync.Map"
    - [ ] Participants ordering
    - [ ] Mermaid: display snapshots only when reading from it
    - [x] proper activation levels: right now there is a bug: if we read/write multiple times, it'll write "activate" even if it is already activated
- [ ] refactor each levels to build on top of previous levels
- [ ] figure out how to make TestDirtyReadsWrites.NewTwoPhaseLocking concurrent
- [ ] logging
