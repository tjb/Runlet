# Replayable Sync Job

A replayable sync job copies records from a source of truth into another system
using an ordered cursor and an idempotent sink.

For example, a service may need to sync changed customers from its database into
a CRM, search index, or analytics table. The database is the source of truth.
The downstream system is updated by stable external ID, so writing the same
customer more than once is safe.

```text
database changes -> validate/filter -> transform -> upsert sink -> checkpoint
```

The important rule is that the checkpoint advances only after the sink commit
succeeds. If the process crashes after the sink write but before the checkpoint
is persisted, the next run may replay the same chunk. That is acceptable when
the sink performs upserts keyed by a stable ID.

```kotlin
import org.aetherlink.runlet.api.CheckpointableSources
import org.aetherlink.runlet.connector.file.FileCheckpointStore
import org.aetherlink.runlet.dsl.Runlet

data class CustomerChange(
    val id: String,
    val email: String?,
    val displayName: String,
    val changeSequence: Long,
)

data class CrmCustomerUpsert(
    val externalId: String,
    val email: String,
    val displayName: String,
)

val source =
    CheckpointableSources.byLongCursor(
        chunkSize = 500,
        read = { afterSequence, limit ->
            customerDao.fetchChangedCustomersAfter(
                sequence = afterSequence,
                limit = limit,
            )
        },
        cursorOf = { customer -> customer.changeSequence },
    )

Runlet("customer-crm-sync") {
    source(source)
        .checkpoint(FileCheckpointStore("state/customer-crm-sync.ckpt"))
        .filter { customer -> customer.email != null }
        .map { customer ->
            CrmCustomerUpsert(
                externalId = customer.id,
                email = customer.email!!,
                displayName = customer.displayName,
            )
        }
        .sink(crmUpsertSink)
}
```

The sink should be implemented as an idempotent upsert, not a blind append or
create-only operation:

```text
externalId = customer.id
operation = create or update
```

That gives the job at-least-once processing with replay safety:

- if the CRM or index is down, the checkpoint does not move forward
- if the process restarts, the job resumes from the last persisted cursor
- if the last chunk is replayed, upserts overwrite the same logical records
- no broker or separate worker fleet is required for this class of job

This pattern also applies to database-to-search-index backfills, account syncs,
read-model rebuilds, and analytics table refreshes where the target can be
updated by stable key.
