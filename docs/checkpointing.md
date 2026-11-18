# Checkpointing

The checkpoint JSON contains the committed binlog file, position, last GTID, and—when GTID restart is enabled—the full executed GTID set. The path is configured by `checkpoint.path`. On first startup, Binflux establishes a baseline at the configured initial file/position or at the source's current end. On each committed transaction, it flushes stdout before persisting the new position and GTID set.

The file is replaced atomically in its own directory and both file and directory are synced. Keep the checkpoint on durable local storage and back it up with the configuration. Do not share a checkpoint between concurrent Binflux processes. Reset requires `binflux checkpoint reset --yes` and causes a fresh run to begin at the current source end unless an initial position is configured.

GTID restart requires `source.gtid_restart: true`, `gtid_mode=ON`, and a checkpoint with a full `gtid_set`. An old checkpoint can continue in file/position mode. To use GTID restart with it, deliberately reset to a new current-end baseline; doing so skips events before that start point. Pluggable checkpoint storage remains future work.
