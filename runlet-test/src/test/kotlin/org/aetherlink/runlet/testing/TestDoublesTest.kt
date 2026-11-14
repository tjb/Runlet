package org.aetherlink.runlet.testing

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.aetherlink.runlet.api.Cursor
import org.aetherlink.runlet.api.CursorRange
import org.aetherlink.runlet.api.RunletRuntimeConfig
import org.aetherlink.runlet.dsl.Runlet
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TestDoublesTest {
    private val letters = listOf("a", "b", "c", "d", "e")

    @Test
    fun `rerun after a failed commit resumes from the last checkpoint`() =
        runBlocking {
            val source = ScriptedCheckpointableSource(letters, chunkSize = 2)
            val sink = RecordingSink<String>().failOnCommit(1)
            val checkpoint = InMemoryCheckpointStore()

            assertFailsWith<InjectedFailure> { checkpointed(source, checkpoint, sink).run() }
            assertEquals(Cursor(2), checkpoint.cursor)
            assertEquals(listOf(listOf("a", "b")), sink.committedChunks)

            checkpointed(source, checkpoint, sink).run()

            assertEquals(listOf(null, Cursor(2)), source.resumeCursors)
            assertEquals(letters, sink.committedRecords)
            assertEquals(listOf(Cursor(2), Cursor(4), Cursor(5)), checkpoint.history)
            assertFalse(source.isOpen)
            assertEquals(2, source.closes)
        }

    @Test
    fun `the same pipeline instance can be rerun after a failure`() =
        runBlocking {
            val source = ScriptedCheckpointableSource(listOf("a", "b", "c", "d"), chunkSize = 2)
            val sink = RecordingSink<String>().failOnCommit(1)
            val checkpoint = InMemoryCheckpointStore()
            val pipeline =
                Runlet("orders") {
                    source(source).checkpoint(checkpoint).sink(sink)
                }

            assertFailsWith<InjectedFailure> { pipeline.run() }
            pipeline.run()

            assertEquals(listOf(null, Cursor(2)), source.resumeCursors)
            assertEquals(listOf("a", "b", "c", "d"), sink.committedRecords)
            assertFalse(source.isOpen)
        }

    @Test
    fun `failed persist after commit replays the committed chunk`() =
        runBlocking {
            val source = ScriptedCheckpointableSource(letters, chunkSize = 2)
            val sink = RecordingSink<String>()
            val checkpoint = InMemoryCheckpointStore().failOnPersist(1)

            assertFailsWith<InjectedFailure> { checkpointed(source, checkpoint, sink).run() }
            checkpointed(source, checkpoint, sink).run()

            assertEquals(listOf("a", "b", "c", "d", "c", "d", "e"), sink.committedRecords)
            val replayed = CursorRange(Cursor(2), Cursor(4))
            assertEquals(2, sink.writes.count { it.cursorRange == replayed })
            assertEquals(Cursor(5), checkpoint.cursor)
        }

    @Test
    fun `failed write leaves the checkpoint unchanged and closes the reader`() =
        runBlocking {
            val source = ScriptedCheckpointableSource(letters, chunkSize = 2)
            val sink = RecordingSink<String>().failOnWrite(0)
            val checkpoint = InMemoryCheckpointStore()

            assertFailsWith<InjectedFailure> { checkpointed(source, checkpoint, sink).run() }

            assertNull(checkpoint.cursor)
            assertTrue(sink.writes.isEmpty())
            assertEquals(1, source.opens)
            assertFalse(source.isOpen)
        }

    @Test
    fun `read failure fails the pipeline and closes the reader`() =
        runBlocking {
            val source = ScriptedSource(letters, chunkSize = 2).failOnRead(1)
            val sink = RecordingSink<String>()

            assertFailsWith<InjectedFailure> {
                Runlet("test") { source(source).sink(sink) }.run()
            }

            // Uncheckpointed stages run concurrently, so the sink may or may not have
            // received the first chunk before the failure cancelled it.
            assertEquals(2, source.readCalls)
            assertFalse(source.isOpen)
            assertEquals(1, source.closes)
        }

    @Test
    fun `uncheckpointed sink writes carry no cursor range`() =
        runBlocking {
            val source = ScriptedSource(letters, chunkSize = 2)
            val sink = RecordingSink<String>()

            Runlet("test") {
                source(source).map { it.uppercase() }.sink(sink)
            }.run()

            assertEquals(listOf("A", "B", "C", "D", "E"), sink.committedRecords)
            assertTrue(sink.writes.all { it.cursorRange == null })
            assertEquals(3, sink.commits)
            assertEquals(4, source.readCalls)
        }

    @Test
    fun `cancelling a running pipeline releases the source reader`() =
        runBlocking {
            val source = ScriptedSource(letters, chunkSize = 1)
            val sink = RecordingSink<String>()
            val blocked = CompletableDeferred<Unit>()

            val job =
                launch {
                    Runlet("test") {
                        source(source)
                            .evalMap { record ->
                                if (record == "c") {
                                    blocked.complete(Unit)
                                    awaitCancellation()
                                }
                                record
                            }.sink(sink)
                    }.run()
                }
            blocked.await()
            job.cancelAndJoin()

            assertEquals(1, source.opens)
            assertFalse(source.isOpen)
            assertFalse("c" in sink.committedRecords)
        }

    @Test
    fun `recording observer captures the pipeline lifecycle`() =
        runBlocking {
            val observer = RecordingObserver()
            val sink = RecordingSink<String>().failOnWrite(1)

            assertFailsWith<InjectedFailure> {
                Runlet("orders", RunletRuntimeConfig(observer = observer)) {
                    source(ScriptedCheckpointableSource(letters, chunkSize = 2))
                        .checkpoint(InMemoryCheckpointStore())
                        .sink(sink)
                }.run()
            }

            val events = observer.events
            assertEquals(
                listOf(ObserverEvent.Started("orders"), ObserverEvent.ChunkCommitted("orders", 2)),
                events.dropLast(1),
            )
            val failed = assertIs<ObserverEvent.Failed>(events.last())
            assertIs<InjectedFailure>(failed.failure)
        }

    @Test
    fun `resuming past the end of the script fails`() {
        val source = ScriptedCheckpointableSource(letters, chunkSize = 2)

        assertFailsWith<IllegalArgumentException> {
            runBlocking { source.useReader(Cursor(6)) { read() } }
        }
        assertEquals(0, source.opens)
    }

    private fun checkpointed(
        source: ScriptedCheckpointableSource<String>,
        checkpoint: InMemoryCheckpointStore,
        sink: RecordingSink<String>,
    ) = Runlet("test") {
        source(source).checkpoint(checkpoint).sink(sink)
    }
}
