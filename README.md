# Transaction simulator

- [ ] PlayEvents
    - [ ] rollback
    - [x] read from snapshot
    - [ ] refactor
        - [ ] Participants class:
            - ensure snapshots exist, but are unique
            - only display snapshots if reading from it
            - sort by the following rules:
              - keep snapshots to the right of the tx it belongs to
              - center rows, destribute txns between the rows
    - [ ] proper log levels: right now there is a bug: if we read/write multiple times, it'll write "activate" even if it is already activated
- [ ] refactor each levels to build on top of previous levels
- [ ] figure out how to make TestDirtyReadsWrites.NewTwoPhaseLocking concurrent
- [ ] logging
