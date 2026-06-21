# Failure Semantics

This document describes Runlet's current v0 checkpoint and replay behavior.

## Scope

These guarantees apply to checkpointed v0 pipelines:

- one checkpointable source
- one linear chain of stages
- one terminal sink
- no branching
- no windowing or grouped state
- one chunk in flight at a time

Uncheckpointed pipelines use bounded concurrent stages, but they do not persist
progress. If an uncheckpointed pipeline fails, the caller decides how to retry.

## Checkpointed Execution Order

For each source chunk, Runlet executes this sequence:

```text
read -> transform -> write -> commit -> persist cursor
```

The cursor persisted is `SourceChunk.cursorRange.next`, which is the position
from which the source should resume on the next run.

Runlet does not read the next source chunk until the current chunk has finished
this full sequence.

## Failure Cases

If `read()` fails:

- no sink write happens for that chunk
- no checkpoint is advanced
- the pipeline fails

If a transform stage fails:

- the sink is not called for that transformed chunk
- no checkpoint is advanced
- the pipeline fails

If `Sink.write()` fails:

- `Sink.commit()` is not called for that chunk
- no checkpoint is advanced
- the pipeline fails

If `Sink.commit()` fails:

- no checkpoint is advanced
- the pipeline fails
- the sink may or may not have partially committed, depending on that sink's own
  implementation

If `CheckpointStore.persist()` fails after `Sink.commit()` succeeds:

- the sink has accepted and committed the chunk
- the checkpoint may still point to the previous cursor
- the next run may replay the committed chunk
- this is the fundamental at-least-once replay case

## At-Least-Once, Not Exactly-Once

Runlet v0 is at-least-once for checkpointed pipelines.

The important crash window is:

```text
Sink.commit() succeeds -> process dies before CheckpointStore.persist() succeeds
```

On restart, Runlet resumes from the last persisted cursor. Because that cursor
did not advance, the in-flight chunk is read and processed again.

Runlet does not claim exactly-once semantics. Sink authors and pipeline authors
must choose sinks that tolerate replay if duplicates matter.

## Sink Commit Contract

`Sink.write(chunk)` means the sink accepted the chunk. It may still be buffered.

`Sink.commit()` means the sink has made the accepted chunk durable according to
that sink's own contract.

Runlet only advances a checkpoint after `commit()` returns successfully.

For a sink with no meaningful durability boundary, the default no-op `commit()`
is allowed, but then checkpoint durability is only as strong as that sink's
behavior. A console sink or in-memory test sink can reasonably use no-op commit;
a filesystem, database, or queue sink should define a real commit boundary.

## Checkpoint Store Contract

`CheckpointStore.persist(cursor)` must return only after the cursor is durable
according to the store's own contract.

If a checkpoint store reports success before the cursor is durable, a crash can
silently resume from an older cursor and replay more work than the caller
expects.

`FileCheckpointStore` uses a temp-write and atomic replace pattern. It also
attempts filesystem syncs, but exact durability depends on the filesystem and
operating system.

## Append Sinks Can Duplicate

Append-only sinks are duplicate-prone under at-least-once replay.

Example:

```text
write lines to output.jsonl
commit succeeds
process dies before checkpoint persist
restart from previous cursor
write same lines to output.jsonl again
```

The result is duplicate output rows.

Append-only sinks are acceptable when duplicates are harmless or handled
downstream. They are not replay-idempotent by themselves.

## Cursor-Range Chunk Files

`ChunkFileSink` avoids append duplication by naming each output file with the
source cursor range:

```text
chunk-0000000000000-0000000001024.jsonl
```

If the same chunk replays, it writes the same final filename. The new temp file
replaces the old final file atomically.

That does not make processing exactly-once. The chunk is still processed again.
It makes the output write replay-idempotent for deterministic transforms.

## Current Guarantees

Runlet v0 guarantees:

- checkpointed pipelines process one chunk at a time
- checkpoints advance only after sink commit returns
- checkpoints do not advance when `write()` or `commit()` throws
- checkpointable sources resume from the last persisted cursor
- source reader scopes are exited on failure or cancellation when the source
  implementation follows the `useReader` contract

## Non-Guarantees

Runlet v0 does not guarantee:

- exactly-once processing
- distributed checkpointing
- transactional coordination across multiple sinks
- checkpointing with windowed or grouped state
- replay safety for arbitrary sinks
- durability stronger than the configured sink and checkpoint store provide
