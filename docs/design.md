# Runlet Design Notes

This document preserves the original design pitch and roadmap. It is broader
than the current implementation. For install instructions and examples that
match the code today, see the root [README.md](../README.md).

> A small, embeddable, batch-oriented stream processing library for the JVM.
> Borrows DuckDB's embeddable, zero-ops deployment philosophy —
> applied to a problem DuckDB doesn't have.
>
> _The name: a small runnable pipeline — embeddable, not cluster-scale._

> **Note:** this document is currently a combined pitch + design doc, not a
> shipping README. When this becomes a repo it should split into:
> `README.md` (pitch, install, v0 example, current scope, non-goals),
> `docs/design.md` (checkpointing, source/sink contracts, phased roadmap,
> the DuckDB analogy details), and `docs/failure-semantics.md`
> (at-least-once, replay, idempotent sinks). Kept together here while the
> design is still being shaped.

## The pitch

DuckDB didn't try to be a smaller Spark. It picked a well-understood execution
model — vectorized, columnar, single-node — and made it embeddable, fast, and
genuinely pleasant to use, with zero operational overhead. No server, no port,
no daemon: you link it as a library and it runs inside your process.

Stream processing has the same gap on the deployment axis. On one side:
`Flow`/collections, fine for small or simple cases. On the other: Flink,
Kafka Streams, Spark Streaming — correct, powerful, and a real operational
commitment (a cluster, a checkpointing backend, a deploy story) most local
pipelines don't need.

Runlet takes DuckDB's deployment philosophy — a dependency, not a deployment;
zero ops; batched execution instead of record-at-a-time — and applies it to
a problem DuckDB was never built for. DuckDB is query-execute-return: a
one-shot call against an embedded engine, with no long-running
producer/consumer pipeline semantics to manage (DuckDB does parallelize
internally within a single query, but that's a different kind of
concurrency from what's below). Runlet is a long-running, concurrent,
stateful pipeline — sources producing, sinks consuming, backpressure
flowing between stages, all inside coroutines in your process, for as
long as the pipeline runs. Same embeddable posture, genuinely different
execution shape underneath.

**What Runlet is not:** event-time semantics, watermarks, exactly-once
distributed checkpointing, or a multi-node execution graph. Those are real,
hard, well-studied problems — and Flink already solves them well. Runlet
deliberately stays single-process and processing-time only. That constraint
is what keeps it small and easy to reason about.

## What you'd actually use this for

- **Tail logs and compute rolling metrics** — global rates and counts
  in v1; per-service/per-key breakdowns in v1.5 (see **Scope, phased**).
  No Flink cluster for a sidecar process either way.
- **Process a large local export with resumable progress** — a job that
  dies at record 4M of 10M resumes at 4M, not 0.
- **Poll an API, normalize records, write output** — backpressure
  handled by the runtime (bounded queues, structural), not hand-rolled
  in the polling loop.
- **Write deterministic tests for streaming business logic** — assert on
  windowed aggregation output without real `delay()` calls or flaky
  timing-dependent tests.

## Hello world

A v0-supported pipeline — read a file, transform each record, write it
out, with resumable checkpointing:

```kotlin
Runlet("orders") {
    source(FileSource.jsonLines<Order>("orders.jsonl"))
        .checkpoint(FileCheckpointStore("orders.ckpt"))
        .filter { it.status == "completed" }
        .map(::toSummary)
        .sink(ChunkFileSink.jsonLines("summaries/"))
}.run()
```

No broker. No cluster. No YAML. `./gradlew run` and it's processing —
and if it dies halfway, the next run resumes instead of starting over.

**Where this is headed** — windowed, grouped aggregation (v1.5; see
**Scope, phased**):

```kotlin
Runlet("order-totals") {
    source(FileSource.jsonLines<Order>("orders.jsonl"))
        .filter { it.status == "completed" }
        .window(tumbling = 1.minutes)
        .groupBy { it.customerId }
        .aggregate { orders -> orders.sumOf(Order::amountCents) }
        .sink(ChunkFileSink.jsonLines("totals/"))
}.run()
```

(The second example combines `window` and `groupBy`, which is v1.5, and
has no `.checkpoint(...)` — checkpointing combined with windowed/grouped
state isn't supported in any phase yet, see **Checkpointing**.)

## Why it's batch-oriented, not record-at-a-time

Most hand-rolled JVM streaming code (and a lot of libraries built on `Flow`)
moves one record at a time through suspend-function overhead at every stage.
DuckDB's speed comes partly from processing column-oriented batches of
~1024-2048 values instead of row-at-a-time. Runlet borrows that batching
discipline and applies it to coroutines instead of CPU vectors: records move
through the pipeline as `Chunk<T>`, not one at a time. `map`/`filter` apply
to a `Chunk<T>` under the hood; you still write per-record lambdas, Runlet
batches the plumbing.

This is a design claim, not a benchmark claim — fewer suspend-point
crossings per record should mean lower overhead, but "should" isn't
"measured." Benchmarks of the core chunked path against naive
record-at-a-time and against `Flow` directly are a v0 goal, not a v0
fact yet.

## Core types

Sources own their resource lifetime explicitly, rather than implementing
against a `ProducerScope`/`Channel` directly — that would make every
third-party source author responsible for getting coroutine/channel
mechanics right, which is exactly the opposite of what this library is
for. The runtime owns the channel plumbing; sources just read and sinks
just write.

```kotlin
// SourceReader is parameterized only by its chunk type. (Dropping the
// separate item-type param: the chunk type already determines the items,
// so a second T would be purely documentary.)
interface SourceReader<C> {
    suspend fun read(): C?
}

interface Source<T> {
    suspend fun <R> useReader(
        block: suspend SourceReader<Chunk<T>>.() -> R,
    ): R
}

// Only checkpointable sources carry resume cursors. A test source, a
// socket source, or a fire-and-forget poll source implements plain
// Source and never has to fabricate a Cursor it doesn't have.
interface CheckpointableSource<T> {
    suspend fun <R> useReader(
        cursor: Cursor?,
        block: suspend SourceReader<SourceChunk<T>>.() -> R,
    ): R
}

data class SourceChunk<T>(
    val chunk: Chunk<T>,
    val cursorRange: CursorRange,
)

// A chunk spans [start, next): start is where this chunk began, next is
// where to resume after it. The sink names output files by this range;
// the checkpoint store persists cursorRange.next.
data class CursorRange(
    val start: Cursor,
    val next: Cursor,
)

interface Sink<T> {
    suspend fun write(chunk: Chunk<T>)
    // commit() is the durability boundary for checkpointed pipelines.
    // Sinks with no meaningful two-phase shape (console, in-memory test
    // sink) may leave it as a no-op — but a checkpointed pipeline's
    // durability is only as strong as its terminal sink's commit().
    suspend fun commit() {}
}

class Pipeline<T> private constructor(/* ... */) {
    fun <R> map(f: (T) -> R): Pipeline<R>
    fun filter(predicate: (T) -> Boolean): Pipeline<T>
    fun <R> evalMap(f: suspend (T) -> R): Pipeline<R>
    fun window(tumbling: Duration): WindowedPipeline<T>
    fun sink(sink: Sink<T>): RunnablePipeline
}
```

**The DSL gates checkpointing at compile time via overloads.** `source(...)`
is overloaded on its argument: a plain `Source<T>` yields a `Pipeline<T>`
with no `.checkpoint(...)` method; a `CheckpointableSource<T>` yields a
`CheckpointablePipeline<T>` that has it. That's how "`.checkpoint(...)` is
compile-time gated" actually holds — it's two return types from two
overloads, not a runtime capability check:

```kotlin
fun <T> source(source: Source<T>): Pipeline<T>
fun <T> source(source: CheckpointableSource<T>): CheckpointablePipeline<T>
```

**Resumability is a type, not a flag.** `.checkpoint(...)` only accepts a
`CheckpointableSource` — a plain `Source` can't be checkpointed, and the
compiler enforces that rather than a runtime "this source doesn't support
cursors" failure. A non-resumable source's reader yields `Chunk<T>`; a
checkpointable one yields `SourceChunk<T>` carrying a real cursor range.
They don't share a subtype relationship — the chunk type is `SourceReader`'s
type parameter, so neither has to pretend to be the other.

**Resource lifetime:** the source must release its resources whenever
`useReader` exits — normally, via exception, or via cancellation. That
guarantee is the implementation's job (`try`/`finally`, `AutoCloseable`,
whatever fits), but it's a hard requirement of implementing `Source`,
not an implementation detail left to chance.

**Durability boundary, made unambiguous:** `write()` means "accepted by
this sink" — it may be buffered. `commit()` means "durable according to
this sink's own contract" — for `ChunkFileSink` that's temp-write +
filesystem sync + atomic rename (see the worked example); for a JDBC
sink that might be after a transaction commits across several buffered
`write()` calls. Sinks with no meaningful two-phase shape (console, the
in-memory test sink) can leave `commit()` as the default no-op — but
then they offer no durability boundary, so a checkpointed pipeline's
guarantee is only ever as strong as its terminal sink's `commit()`. The
runtime only ever advances a checkpoint past a chunk after that chunk's
`commit()` has returned successfully. If `write()` or `commit()` throws,
the runtime fails the chunk (or applies a configured retry policy, if
one is set). Retry is deliberately minimal in v0 — automatic retry
around `commit()` after an unknown partial success, or around `write()`
for a non-idempotent sink, has duplicate implications that aren't
designed yet, so v0 defaults to failing the chunk rather than silently
retrying. See **Checkpointing** below for exactly when `commit()` gets
called relative to cursor advancement.

**Resolved:** the earlier draft had a standalone `SourceReader.cursor()`
method, with an open question about whether it meant "last read" or
"next to read." `SourceChunk.cursorRange` replaces it — the range
(`start` and `next`) travels with the chunk that produced it, returned
atomically by `read()`, so there's no separate method call to be
ambiguous about. The checkpoint store persists `cursorRange.next` —
unambiguously where to resume on restart — and the sink uses both ends
to name its output (see the worked example). The range only exists on a
checkpointable source's chunks (`SourceReader<SourceChunk<T>>`), so
non-resumable sources never deal with it at all.

**The checkpoint store has its own durability contract, separate from
the sink's.** `persist(cursor)` only returns once the cursor is durable
according to the store — for `FileCheckpointStore`, that's the same
temp-write + filesystem sync + atomic rename discipline as
`ChunkFileSink`, not just a buffered write. Otherwise the
"commit sink → persist cursor" sequence would have a precise first half
and a vague second half: a cursor that's "written" but not durable can
be lost in the same crash that the checkpoint exists to survive,
silently resuming from an older position and reprocessing more than the
in-flight chunk.

Backpressure is structural, not bolted on: every stage is a bounded
`Channel`, and a slow sink naturally suspends the stage upstream of it.
No unbounded _runtime_ queues, by construction — Runlet guarantees its own
inter-stage queues are bounded, though it can't stop a third-party
source or sink client from buffering internally.

**One honest caveat about v0 checkpointed pipelines specifically:** the
sequential rule above (one chunk in flight: read → transform → write →
commit → persist) deliberately chooses correctness over pipeline
parallelism. In that mode there isn't much inter-stage backpressure to
speak of, because there's effectively one chunk moving through the
pipeline at a time — chunk N+1 isn't being read while chunk N commits.
Structural backpressure and bounded inter-stage queues are central to
the _broader_ runtime (non-checkpointed pipelines, and later windowing/
concurrent-stage execution), not to the v0 checkpointed path, which is
intentionally serial. The two modes will eventually need to be explicit
about this difference; for now, just don't read "backpressure between
every stage" as implying parallelism inside a checkpointed v0 run.

## Checkpointing

The other thing a stranger's pipeline needs that `Flow` doesn't give you:
resuming after a crash without reprocessing everything.

```kotlin
Runlet("orders") {
    source(FileSource.jsonLines<Order>("orders.jsonl"))
        .checkpoint(FileCheckpointStore("orders.checkpoint"))
        .map(::normalize)
        .sink(FileSink.jsonLines("normalized.jsonl")) // see caveat below
}.run()
```

(This snippet uses a plain append sink for brevity. As the worked example
further down explains, pairing checkpointing with an append-only sink is
duplicate-prone on crash-replay — a cursor-named chunk-file sink or any
replay-idempotent sink is the safer choice in practice. Keeping it simple
here to show the wiring, not as a recommendation.)

**The mechanism:** `read()` returns a `SourceChunk<T>` carrying both the
data and a `cursorRange` (`start`, `next`) — `next` is where to resume if
the pipeline restarts after this chunk. v0 processes a checkpointed
pipeline strictly one chunk at a time, with no concurrent chunks in
flight: `read()` the chunk, transform it, `write()` it to the terminal
sink, `commit()` the sink, then persist `cursorRange.next` to the
checkpoint store (durably — see the store's contract above) — in that
order, before the next `read()`. Because exactly one chunk is ever in
flight, `commit()` unambiguously commits that one chunk, not an internal
buffer of several.

If the process dies, the pipeline resumes from the last persisted
cursor. With v0's per-chunk persistence that's at worst reprocessing the
chunks after the last persisted cursor — normally just the single
in-flight chunk. Note the careful wording: it's "after the last
persisted cursor," not "exactly one chunk." If a future phase batches
cursor persistence or pipelines chunks concurrently, more than one chunk
could replay; v0's sequential rule is what keeps it to one, and that's a
property of the rule, not a free guarantee.

**Checkpointing ships in v0 and is deliberately restricted to this
case: a linear pipeline, one source, one terminal sink, ordered
chunks, no windowing.** That restriction is load-bearing, not
incidental. Once you introduce branching, multiple sinks, parallel
stages, reordering, or windowed/grouped state that spans a checkpoint
boundary, "the sink committed so the cursor is safe to persist" stops
being well-defined — which sink? what about partial success across two
sinks? what about a window that's still accumulating when the cursor
would advance? Those are real, hard problems with no honest cheap
answer. v0 doesn't attempt them, and checkpointing combined with
`window`/`groupBy` isn't planned for v1 or v1.5 either — it needs a
real answer for the window/checkpoint interaction first, whenever that
gets designed.

**This means at-least-once, not exactly-once.** A crash between sink
`commit()` succeeding and the cursor persisting reprocesses that chunk.
Sinks should be idempotent if duplicates matter for your use case — for
example, an `UPSERT` instead of an `INSERT`, or a dedup-by-key step
before the sink. This is the sink author's responsibility, not something
the runtime papers over. Runlet commits to being honest about this rather
than implying exactly-once it can't deliver.

## Worked example: resumable processing of a large local export

A data engineer gets a 12GB JSONL export of partner records dropped into
a shared directory nightly. Each line needs validating, normalizing, and
writing to a clean output dataset. The file is too big to comfortably
reprocess from scratch if something fails halfway — a flaky downstream
dependency, an out-of-memory kill, a deploy that restarts the box — and
standing up Spark for a single nightly file is not worth it.

```kotlin
data class PartnerRecord(val id: String, val email: String, val plan: String)
data class NormalizedRecord(val id: String, val email: String, val planTier: Int)

fun parsePartnerRecord(raw: PartnerRecord): NormalizedRecord =
    NormalizedRecord(
        id = raw.id,
        email = raw.email.trim().lowercase(),
        planTier = planTierOf(raw.plan),
    )

fun main() = runBlocking {
    Runlet("partner-feed-normalize") {
        source(FileSource.jsonLines<PartnerRecord>("feeds/incoming/partners.jsonl"))
            .checkpoint(FileCheckpointStore("feeds/.checkpoints/partners.ckpt"))
            .map(::parsePartnerRecord)
            // writes one file per chunk, named by the chunk's cursor range
            .sink(ChunkFileSink.jsonLines("feeds/processed/partners/"))
    }.run()
}
```

`ChunkFileSink` writes each chunk to its own file named by the chunk's
cursor range. Its `commit()` is a temp-write + filesystem sync + atomic
rename, so a reader never sees a partial file:

```
# during write() — partial, not yet visible as final:
feeds/processed/partners/chunk-0000008400000-0000008410240.jsonl.tmp

# after commit() — synced, then atomically renamed:
feeds/processed/partners/chunk-0000008400000-0000008410240.jsonl
```

The atomic rename is the concrete meaning of this sink's durability
contract: `commit()` returning successfully means the final filename
exists and its contents are durable on disk. (Implementation note: a
truly durable rename usually means syncing the file _and_ the parent
directory after the rename, depending on the filesystem — the README
says "filesystem sync" rather than naming exact syscalls because getting
this right is the implementation's job, not a detail to over-specify in
prose.) A crash mid-`write()` leaves only a `.tmp` file, which the next
run overwrites — it never corrupts or half-publishes the real output.

What happens on a crash: say the process is killed at record 8.4M of
12M. On restart, `FileCheckpointStore` reports the last persisted
cursor — the `cursorRange.next` (a byte offset) of the last chunk whose
`commit()` succeeded. `FileSource.jsonLines` seeks to that offset and
resumes reading from there. The job picks up at ~8.4M, not 0. The chunk
filenames come directly from `cursorRange`: `start` and `next` are the
two numbers in `chunk-<start>-<next>.jsonl`.

**Why this sink, and not plain append-only JSONL:** v0 is at-least-once,
so a crash between `commit()` and cursor-persist replays the in-flight
chunk. With a plain append sink, that replay produces duplicate lines —
append is not idempotent. The chunk-file pattern sidesteps that: the
replayed chunk has the same cursor range, so it writes the _same
filename_ (via the same temp-write + atomic-rename), overwriting rather
than appending duplicates. At-least-once processing becomes effectively
idempotent _output_ without the sink needing to be transactional.
Downstream reads the directory as one logical dataset (glob the chunk
files); a manifest of committed cursor ranges can make "which chunks are
durably done" explicit if you want it.

This isn't exactly-once — the chunk genuinely is processed twice on that
crash path. It's that the _second_ processing is harmless because it's a
deterministic atomic overwrite, not an append. That's the honest version
of "resumable local export": pick a sink whose write is idempotent under
replay, rather than pretending replay won't happen.

This is also exactly the shape v0 is built for and nothing more: one
source, one linear chain of `map`, one terminal sink, no windowing. The
moment this needed a `groupBy` (say, aggregating partner records by
`plan` before writing), it moves into v1.5 — see **Scope, phased**
below — and combining that with checkpointing isn't supported at all
yet.

## Scope, phased

Checkpointing and windowed/grouped state are each a real subsystem on
their own — checkpointing needs an unambiguous durability contract
(see **Core types**), windowing needs state maps, timers, virtual-time
integration, emission ordering, and memory bounds for high-cardinality
keys. Shipping both at once as "v1" was asking for two hard problems
to land simultaneously. Phased instead:

**v0 — the spine:** single process, single source, linear pipeline
(`source -> map/filter/evalMap -> sink`), chunked execution, bounded
queues between every stage, graceful cancellation, checkpointing for
ordered linear pipelines as described above. No windowing at all. This
is the smallest version that's still honestly useful — the worked
example above is entirely v0.

v0 has its own test harness, and it's already a differentiator even
without virtual time: drive a source through scripted chunks, fail the
sink on chunk N, restart from checkpoint, and assert the chunk-file sink
overwrites the replayed range instead of duplicating it; separately,
cancel mid-pipeline and assert the source released its resources. Those
crash/replay and cancellation tests don't need a time abstraction —
they're the tests that prove the v0 checkpoint and resource contracts
actually hold.

**v1 — processing-time windows + the deterministic _time_ harness:** adds
`window(tumbling = ...)` and extends the test harness with virtual time
(scripted input, advance the clock, assert-on-output) — the part that
makes _time-dependent_ pipeline tests deterministic, on top of the v0
crash/replay harness. Still no `groupBy`/aggregation and no checkpointing
combined with windowing — those interact in ways that don't have a cheap
honest answer yet (a window mid-accumulation when a checkpoint would
commit is exactly the unresolved case from **Checkpointing** above).

**v1.5 — grouped aggregation:** adds `groupBy` + `aggregate` on top of
windowing. This is where the "where this is headed" example under
**Hello world** actually becomes fully supported, including its implicit
assumption that windowing and grouping compose cleanly without
checkpointing.

**Explicitly out of scope, maybe later, maybe never:** event-time
windows, watermarks, Kafka/JDBC connectors, exactly-once semantics,
distributed execution, a dashboard, checkpointing combined with
windowed/grouped state, branching, multiple sinks, generic parallelism.

## The real differentiator: deterministic tests for concurrent pipelines

This is the part nobody gets right by hand. `Flow` gives you operators
and suspension-based backpressure — it's a primitive. Runlet is a pipeline
runtime built on top of that primitive: chunking, resumable sources,
bounded stage queues, sink lifecycle, checkpoint policy, and — the part
that matters most here — deterministic tests for all of it.

The harness lands in two stages. **v0** ships crash/replay and
cancellation tests (no time abstraction needed): script a source, fail
the sink on a chosen chunk, restart, and assert the checkpoint resumes
correctly and the chunk-file sink overwrites rather than duplicates;
cancel mid-pipeline and assert resource cleanup. **v1** adds the
deterministic _time_ layer on top — virtual time for windowing, so
_time-dependent_ tests stop being flaky:

```kotlin
@Test
fun `processing-time window emits after boundary crossed`() = pipelineTest {
    val input = testSource<Order>()
    val output = testSink<OrderTotal>()

    Runlet("test") {
        input.window(tumbling = 1.minutes)
            .aggregate { it.sumOf(Order::amountCents) }
            .sink(output)
    }.runInTest()

    input.emit(orderOf(customer = "a", amount = 500))
    advanceTimeBy(45.seconds)
    input.emit(orderOf(customer = "a", amount = 700))
    advanceTimeBy(20.seconds) // crosses the 1-minute boundary

    output.assertEmitted(OrderTotal(total = 1200))
}
```

Virtual time, no real `delay()`, no flakiness, fully deterministic. This
is the harness DuckDB doesn't need (it's not concurrent) and most
streaming libraries don't bother building well. Note the test name says
"processing-time window," not "late completion" — v1 has no event-time
semantics, so nothing here should imply otherwise.

v1.5 extends this same harness to `groupBy` + windowed aggregation —
the per-customer version, like the "where this is headed" example under
**Hello world** — once that combination is actually built.

## Where the analogy stops

To be precise about what's borrowed and what isn't: DuckDB has no concurrent
producers/consumers to manage — it's not a server, it doesn't multiplex
connections, a query runs and returns. Runlet's actual hard problem is the
opposite: a standing pipeline with multiple coroutines running concurrently,
backpressure between them, and state that needs to survive a crash. That part
has no DuckDB equivalent. What Runlet borrows from DuckDB is the _posture_
(embeddable, zero-ops, batched execution) — not the execution model itself.

## Embedding it safely (it runs in your process)

Because Runlet runs inside your app's JVM rather than as a separate service,
it shares that process's memory and threads — the same tradeoff DuckDB
makes, and the same one that means "it's just a library" isn't the whole
story. A pipeline can degrade or, worst case, take down its host process
if you ignore three things. All three are bounded and configurable; the
point of this section is that the safe path is a choice you make on
purpose, not a default you get for free.

**Memory — bound what's in flight.** The runtime is bounded by
construction (bounded channels, chunked execution, no unbounded queues),
so steady-state memory is roughly chunk size × pipeline depth × any
sink-side batching. The failure mode is setting a large chunk size, or
running many pipelines at once, inside a heap already mostly consumed by
the rest of your app — then you OOM the process. Pick chunk size with the
host's heap in mind; this is the direct analog of DuckDB's
`memory_limit`. The risk isn't absent, it's capped and yours to set.

**Threads — give it its own dispatcher.** Pipelines run on coroutines,
which run on a dispatcher backed by a thread pool. In a shared server
process, don't run pipelines on the app's default dispatchers
(`Dispatchers.IO`, `Dispatchers.Default`) — a long-running pipeline can
pin those threads and starve the rest of the process (in a web app, that
means request-handling threads). Give Runlet a dedicated, bounded
dispatcher sized on purpose. (For a throwaway CLI or a test, the default
dispatchers are fine — the rule is about _shared_ processes where Runlet
competes with something that matters.) This is the failure worth worrying
about more than memory, because it shows up as intermittent latency
rather than a clean crash.

**Lifecycle — tie it to your container's shutdown.** On deploy your
process gets `SIGTERM`. The pipeline's scope must be cancelled as part of
that shutdown so in-flight work stops cleanly and — critically —
`useReader`'s resource cleanup actually runs (open files/sockets get
released). Runlet's cancellation contract guarantees cleanup _if_ the
pipeline's `CoroutineScope` is wired to your app's lifecycle and actually
cancelled. If you launch a pipeline in a scope nothing ever cancels, a
shutdown mid-chunk leaks the source's handles.

### Two deployment shapes

The cleanest way to decide isn't a config flag, it's an architecture
call:

- **In-process, same JVM as the app.** Simplest — no new deployable, the
  pipeline rides along. Fine for low-volume, non-critical batch work. The
  cost is coupling: a heavy pipeline run competes for the app's heap and
  threads and can degrade request latency.
- **Separate worker process, same jar.** Run the same build with the web
  stack disabled (e.g. Spring's `spring.main.web-application-type=none`)
  so only the pipeline starts, deployed as its own process. Same code,
  isolated resources — "the pipeline OOM'd a pod that does nothing but
  pipelines" instead of "the pipeline OOM'd the app." This is where the
  "embedded, not a cluster" pitch is strongest: it's still one extra
  process running your own jar, not a distributed system.

### Spring Boot, sketched (not yet a real starter)

There's no `runlet-spring-boot-starter` yet; this is the shape the
integration would take — a dedicated dispatcher, a lifecycle-bound scope,
and a `SmartLifecycle` bean whose `stop()` is allowed to block until
graceful shutdown finishes. **This is sketch code — copy the structure,
not necessarily the exact details; the inline notes flag what real
starter code would tighten:**

```kotlin
@Configuration
class RunletConfig {
    // Dedicated bounded dispatcher — NOT the app's default dispatchers.
    // Non-daemon threads: with Spring-managed shutdown you want graceful
    // stop, not threads that can vanish abruptly if lifecycle wiring fails.
    // Unique thread names (runlet-pipeline-1, -2, ...) so thread dumps are
    // actually readable — a shared name makes them useless under load.
    @Bean(name = ["runletDispatcher"], destroyMethod = "close")
    fun runletDispatcher(): ExecutorCoroutineDispatcher {
        val counter = AtomicInteger(0)
        return Executors.newFixedThreadPool(4) { r ->
            Thread(r, "runlet-pipeline-${counter.incrementAndGet()}")
        }.asCoroutineDispatcher()
    }

    // Qualify the injection — an app may have several CoroutineDispatcher
    // beans, and unqualified injection could grab the wrong one. Inject the
    // concrete ExecutorCoroutineDispatcher type for clarity too.
    @Bean
    fun runletScope(
        @Qualifier("runletDispatcher") dispatcher: ExecutorCoroutineDispatcher,
    ): CoroutineScope = CoroutineScope(SupervisorJob() + dispatcher)
}

@Component
class PartnerFeedPipeline(private val scope: CoroutineScope) : SmartLifecycle {
    private var job: Job? = null

    override fun start() {
        if (job?.isActive == true) return // guard against double-start
        job = scope.launch {
            Runlet("partner-feed-normalize") {
                source(FileSource.jsonLines<PartnerRecord>("feeds/incoming/partners.jsonl"))
                    .checkpoint(FileCheckpointStore("feeds/.checkpoints/partners.ckpt"))
                    .map(::parsePartnerRecord)
                    .sink(ChunkFileSink.jsonLines("feeds/processed/partners/"))
            }.run()
        }
    }

    // Real starter code would implement stop(callback: Runnable) so Spring
    // can coordinate shutdown phases; the blocking form is fine for a sketch.
    // The withTimeout matters: if a source ignores cancellation, an unbounded
    // cancelAndJoin() would hang app shutdown — bound it (this is what the
    // RunletRuntimeConfig.shutdownTimeout below should eventually feed).
    override fun stop() {
        runBlocking {
            withTimeoutOrNull(30.seconds) { job?.cancelAndJoin() }
        }
    }

    override fun isRunning() = job?.isActive == true
}
```

A real starter would almost certainly also wire pipeline state into a
Spring Boot `HealthIndicator`, so a failed pipeline surfaces as degraded
health rather than only flipping `isRunning()` — which connects directly
to the failure-policy point below.

**Lifecycle integration also needs a failure policy — the sketch above
doesn't have one.** Note that `SupervisorJob` means a pipeline failure
won't cancel sibling pipelines or the parent scope: the pipeline just
stops, `isRunning()` flips to false, and the app keeps serving traffic as
if nothing happened. Sometimes that's exactly right; often it isn't. Real
integration has to decide, on purpose, what a failed pipeline means —
fail app startup if it can't start, expose health as degraded/down,
auto-restart the pipeline, or log-and-stop just that pipeline. Pick one;
don't inherit silent-failure by default.

The `stop()` graceful-shutdown behavior leans entirely on Runlet's
cancellation-cleanup contract holding under a real `cancelAndJoin()` —
which is exactly what the v0 crash/replay and cancellation tests exist to
prove. Correct in the design, unverified until that test runs.

**A note on where the knobs should live.** The memory and thread
decisions above are currently scattered across how you construct things.
Runlet should eventually expose them as one explicit runtime config, so the
operational-safety story is a single object you set deliberately rather
than a set of conventions to remember:

```kotlin
RunletRuntimeConfig(
    chunkSize = 1024,
    channelCapacity = 4,
    dispatcher = runletDispatcher,
    shutdownTimeout = 30.seconds,
)
```

(Not built yet — but the fact that "safe embedding" decomposes into a
handful of named, bounded knobs is the point. None of this is magic; it's
config you own.)

## Calling it from Java

A large share of the Spring Boot monoliths this targets are Java-only, and
"Kotlin-first" makes a Java team reasonably nervous. The honest answer:
using Runlet from Java works, but the two most distinctive parts of the
design — coroutine `suspend` functions and the type-safe DSL — are also
the parts that degrade most at the Java boundary. So it's smooth for the
data types and the wiring, and deliberately plainer for extension and
construction.

**What's Java-clean already.** The plain data types — `Chunk`,
`SourceChunk`, `CursorRange`, `Cursor` — are Kotlin `data class`es that
compile to ordinary JVM classes with getters, so Java sees
`getCursorRange()` and friends and never knows the difference. Any
non-`suspend` interface method is equally clean.

**What's sharp: `suspend` functions.** A Kotlin `suspend fun` isn't a
normal method to Java — the compiler rewrites it with a hidden
`Continuation` parameter, and calling it from Java directly means handling
coroutine internals by hand, which nobody does. As specced, `Source`,
`Sink`, and `SourceReader` are all `suspend`, so a Java team can't
meaningfully implement them — and implementing custom sources/sinks (their
own DB sink, their own queue source) is exactly what a real adopter wants.
**v0's answer: blocking sibling interfaces for Java authors.** Expose a
`JavaSink` with plain blocking `write(chunk)` / `commit()` and a
`JavaSource` with a blocking `read()`, which the runtime adapts internally.
A Java author writes ordinary blocking methods; Runlet bridges to coroutines
on its side of the fence. (A `CompletableFuture`-based variant is the more
non-blocking option and can come later; blocking is the least-overhead
choice for v0's IO-heavy workload, and — see virtual threads below — it
isn't even a real cost on a modern JVM.)

**What's sharp: the DSL.** The fluent `Runlet { source(...).map(...)
.checkpoint(...).sink(...) }` builder leans on Kotlin trailing lambdas,
receiver scopes, and the overload-based compile-time `.checkpoint()`
gating — none of which translate to Java. Java callers get a plainer
explicit builder (`PipelineBuilder.source(x).map(f).sink(y).build()`)
that does the same thing with less sugar and without the compile-time
checkpoint gating. That's an acceptable trade as long as it's a decision:
the DSL is a Kotlin-caller luxury, not the only sanctioned way to build a
pipeline.

**The wiring layer is fine.** The Spring lifecycle integration above can
be written in Java, except for the one `scope.launch { pipeline.run() }`
line — for which a Java team writes a single thin Kotlin "launcher" class
and keeps everything else Java. One Kotlin file at the boundary is a very
different ask than "rewrite your service in Kotlin."

### Virtual threads (Java 21+), and why they fit Runlet well

A common worry: "Kotlin uses coroutines, Java 25 has virtual threads —
do these conflict?" They don't. Coroutines and virtual threads are two
solutions to the same problem (cheap concurrency without OS-thread cost)
at different layers, and they compose rather than compete — coroutines
were designed to run on _any_ concurrency engine, including Loom. The seam
is the dispatcher, which you already control.

Runlet's dispatcher is swappable, so on Java 21+ you can back it with a
virtual-thread executor:

```kotlin
// Instead of newFixedThreadPool(4) in the Spring sketch above:
Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
```

This is a genuinely good fit for v0 specifically, because v0 is
blocking-IO-heavy by nature — reading a 14GB file, fsync-ing chunk files.
On a fixed platform-thread pool, those blocking operations tie up real
threads; on virtual threads, blocking is cheap, so the file work doesn't
starve anything. Runlet doesn't need to "support virtual threads" as a
feature — it gets them for free because the dispatcher is just a
`CoroutineDispatcher`, and on Java 21+ that dispatcher can be virtual-
thread-backed.

This is also where the Java story and the virtual-thread story turn out to
be the _same_ story: **blocking Java sink interfaces + a virtual-thread
dispatcher is the recommended shape for a Java-only Java 21+ codebase.** A
Java team writes an ordinary blocking sink, Runlet runs it on virtual
threads so the blocking is cheap, and they never see a coroutine or a
`suspend` function. The coroutine machinery stays entirely on Runlet's side
of the fence.

## Non-goals, restated

If you need event-time correctness, exactly-once across a cluster, or
horizontal scale: use Flink or Kafka Streams. Runlet is for the much more
common case — a single JVM process, a real need for windowing/backpressure/
checkpointing, and no appetite for standing up infrastructure to get them.

## Status

Early development. API will change. Feedback and issues welcome.
