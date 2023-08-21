# Pageserver generation numbers for safe failover & tenant migration

## Summary

A scheme of logical "generation numbers" for pageservers and their attachments is proposed, along with
changes to the remote storage format to include these generation numbers in S3 keys.

Using the control plane as the issuer of these generation numbers enables strong anti-split-brain
properties in the pageserver cluster without implementing a consensus mechanism directly
in the pageservers.

## Motivation

Currently, the pageserver's remote storage format does not provide a mechanism for addressing
split brain conditions that may happen when replacing a node during failover or when migrating
a tenant from one pageserver to another. From a remote storage perspective, a split brain condition
occurs whenever two nodes both think they have the same tenant attached, and both can write to S3. This
can happen in the case of a network partition, pathologically long delays (e.g. suspended VM), or software
bugs.

This blocks robust implementation of failover from unresponsive pageservers, due to the risk that
the unresponsive pageserver is still writing to S3.

### Prior art

- 020-pageserver-s3-coordination.md
- 023-the-state-of-pageserver-tenant-relocation.md
- https://www.notion.so/neondatabase/Proposal-Pageserver-MVCC-S3-Storage-8a424c0c7ec5459e89d3e3f00e87657c

This RFC has broad similarities to the proposal to implement a MVCC scheme in
S3 object names, but this RFC avoids a general purpose transaction scheme in
favour of more specialized "generations" that work like a transaction ID that
always has the same lifetime as a pageserver process and/or tenant attachment.

## Requirements

- Accommodate storage backends with no atomic or fencing capability (i.e. work within
  S3's limitation that there are no atomics and clients can't be fenced)
- Don't depend on any STONITH or node fencing in the compute layer (i.e. we will not
  assume that we can reliably kill and EC2 instance and have it die)
- Enable per-tenant granularity migration/failover, so that the workload after a failure
  is spread across a number of peers, rather than monolithically moving all load from
  a particular server to some other server (we do not rule out the latter case, but should
  not constrain ourselves to it).

## Design Tenets

These are not requirements, but are ideas that guide the following design:

- Avoid implementing another consensus system: we already have a strongly consistent
  database in the control plane that can do atomic operations where needed, and we also
  have a Paxos implementation in the safekeeper.
- Avoiding locking in to specific models of how failover will work (e.g. do not assume that
  all the tenants on a pageserver will fail over as a unit).
- Avoid doing synchronization that scales with the number of tenants, unless absolutely
  necessary.
- Be strictly correct when it comes to data integrity. Occasional failures of availability
  are tolerable, occasional data loss is not.

## Non Goals

The changes in this RFC intentionally isolate the design decision of how to define
logical generations IDs and object storage format in a way that is somewhat flexible with
respect to how actual orchestration of failover works.

This RFC intentionally does not cover:

- Failure detection
- Orchestration of failover
- Standby modes to keep data ready for fast migration
- Intentional multi-writer operation on tenants (multi-writer scenarios are assumed to be transient split-brain situations).
- Sharding.

## Impacted Components

pageserver, control plane, safekeeper (optional)

## Implementation Part 1: Correctness

### Summary

- **Generation numbers** are introduced for pageserver node lifetimes and tenant attachments.

  - node generation increments each time a pageserver starts, before it can write to S3
  - attachment generation increments each time the control plane modifies a tenant (`Project`)'s assigned pageserver (an _attachment_'s lifetime is the association between a tenant and a pageserver)
  - the two generations are independent: a pageserver may restart while keeping the same
    attachment generation numbers, and attachments may be changed while a pageserver's
    generation number stays the same.

- **Object keys are suffixed** with the generation numbers
- **Safety in split brain for multiple nodes running with
  the same node ID** is provided by the pageserver node generation in the object key: the concurrent nodes
  will not write to the same key.
- **Safety for multiply-attached tenants** is provided by the
  tenant attach generation in the object key: the competing pageservers will not
  try to write to the same keys.
- **Safety for deletions** is provided by pageservers enqueing deletions until calling out to the control plane to validate that the node & attachment generation numbers have not changed since the deletions were enqueued.
- **The control plane is used to issue generation numbers** to avoid the need for
  a built-in consensus system in the pageserver, although this could in principle
  be changed without changing the storage format.
- **The safekeeper may refuse RPCs from zombie pageservers** via the node generation number.

### Generation numbers

Two logical entities will get new "generation numbers", which are monotonically increasing
integers with a global guarantee that they will not be re-used:

- Pageservers: a generation ID is acquired at startup and lasts until the process ends or another
  process with the same node ID starts.
- Tenant attachments: for a given tenant, each time its attachment is changed, a per-tenant generation
  number increases.

This provides some important invariants:

- If there are multiple pageservers running with the same node ID, they are guaranteed to have
  a different generation ID.
- If there are multiple pageservers running with the same node ID, we may unambiguously know which
  of them "wins" by picking the higher generation number.
- If two pageservers have the same tenant attached, they are guaranteed to have different attachment
  generation numbers.

The node generation number defines a global "in" definition for pageserver nodes: a node whose
generation matches the generation the control plane most recently issued is a member of the cluster
in good standing. Any node with any older generation ID is not.

Distinction between generation numbers and Node ID:

- The purpose of a generation ID is to provide a stronger guarantee of uniqueness than a Node ID.
- Generation ID is guaranteed to be globally unique across space and time. Node ID is not unique
  in the event of starting a replacement node after an original node is network partitioned, or
  in the event of a human error starting a replacement node that re-uses a node ID.
- Node ID is written to disk in a pageserver's configuration, whereas generation number is
  received at runtime just after startup (this does incur an availability dependency, see [availability](#availability)).
- The two concepts could be collapsed into one if we used ephemeral node IDs that were only ever used one time,
  but this makes it harder to efficiently handle the case of the same physical node restarting, where we may want
  to retain the same attachments as it had before restart, without having to map them to a different node ID.
- We may in future run the pageserver in a different cluster runtime environment (e.g. k8s) where
  guaranteeing that the cluster manager (e.g. k8s) doesn't run two concurrent instances with the same
  node ID is much harder.

#### Why use per-tenant _and_ per-node generation numbers?

The most important generation number is the tenant attachment number: this alone would be sufficient
to implement safe migration and failover, if we assume that our control plane will never concurrently
run two nodes with the same node ID.

Running two nodes with the same ID might sound far-fetched, but it would happen very easily if we
ran the pageserver in a k8s environment with a StatefulSet, as k8s provides no guarantee that
an old pod is dead before starting a new one.

The node generation is useful other ways, beyond it's core correctness purpose:

- Including both generations (and the node ID) in object keys also provides some flexibility in
  how HA should work in future: we could move to a model where failover can happen within one
  attachment generation number (i.e. without control plane coordination) without changing the
  storage format, because node IDs/generations would de-conflict writes from peers.
- The node generation is also useful for managing remote objects which are not per-tenant,
  such as the persistent per-node deletion queue which is proposed in this RFC.
- node generation numbers enable building fencing into any network protocol (for example
  communication with safekeepers) to refuse to communicate with stale/partitioned nodes.

Node that the two generation numbers have a different behavior for stale generations:

- A pageserver with a stale generation number should immediately terminate: it is never intended
  to have two pageservers with the same node ID.
- An attachment with a stale generation number is permitted to continue operating (ingesting WAL
  and serving reads), but not to do any deletions. This enables a multi-attached state for tenants,
  facilitating live migration (this will be articulated in the next RFC that describes HA and
  migrations).

### Object Key Changes

#### Generation suffix

All object keys (layer objects and index objects) will contain a numeric suffix that
advances as the attachment generation and node generation advance.

This suffix is the primary mechanism for protecting against split-brain situations, and
enabling safe multi-attachment of tenants:

- Two pageservers running with the same node ID (e.g. after a failure, where there is
  some rogue pageserver still running) will not try to write to the same objects.
- Multiple attachments of the same tenant will not try to write to the same objects, as
  each attachment would have a distinct attachment generation.
  To avoid coupling our storage format tightly with the way we manage generation numbers,
  they will be packed into a single u64 that must obey just the following properties:

- On a node restart, the suffix must increase
- On a change to the attachment, the suffix must increase
- Sorting the suffixes numerically must give the most logically recent data.

```
000000 0000 000000
^^^^^^--------------------- 24 bit attachment generation

       ^^^^---------------- 16 bit node ID

            ^^^^^^--------- 24 bit node generation
```

Hereafter, when referring to "the suffix", we mean this u64 composite that contains the
attachment generation and the node generation. Suffixes are written in a human-frendly
form with dashes between the elements.

For example, we would write the suffix for attachment generation 7 to node ID 0 with node generation 1
like so, while the actual object suffix would be packed into a u64:

```
   00000007-0000-00000001

```

The semantic meaning of these bits may change as the system design evolves: for example, if we
switched to a model of ephemeral node IDs that changed on each startup, then the
way we compose the suffix would change, but the actual object & index format would
remain the same (it just sees an opaque u64).

The fixed number of bits for each field is used to provide a fixed key length. The justification
for the lengths being sufficient are:

- 24 bit generations are enough for the generation to increment 9000 times per day over
  a 5 year system lifetime (in practice generation increments are expected to be far rarer,
  perhaps of the order of 1 per day if we are dynamically balancing load)
- 16 bit node ID is enough for 35 new pageservers to be deployed every day over a 5 year
  system lifetime. In practice the frequency is likely to be more like 1 per week, and

### Deletion Part 1: Remote object deletions

While writes are de-conflicted by writers always using their own generation number in the key,
deletions are slightly more challenging: if a pageserver A is isolated, and the true active node is
pageserver B, then it is dangerous for A to do any object deletions, even of objects that it wrote
itself, because pageserver's B metadata might reference those objects.

To solve this without doing excessive synchronization, deletions are accumulated in deletion lists
(also known as deadlists, or garbage lists), to be executed later. The actual execution of one of
these lists has two preconditions:

- That the executing node has written out the metadata index file since it decided to do
  the deletion (i.e. no to-be-deleted files are referenced by the index)
- That after writing that index file, the deleting node has confirmed that it is still the latest
  generation (i.e. it is the legitimate holder of its node ID), _and_ that its attachments for the tenants
  whose data is being deleted are current.

The order of operations for correctness is:

1. Decide to delete one or more objects
2. Write updated metadata (this means that if we are still the rightful leader, any subsequent leader will
   read metadata that does not reference the about-to-be-deleted object).
3. Confirm we hold a non-stale generation for the node and the attachment(s) doing deletion.
4. Actually delete the objects

Note that at stage 3 we are only confirming that deletions of objects no longer referenced
by the metadata written in step 2 are safe. While Step 3 is in flight, new deletions may
be enqueued, but these are not safe to execute until a subsequent iteration of the
generation check.

Because step 3 puts load on the control plane, deletion lists should be executed lazily,
accumulating many deletions before executing them behind a batched generation check message
to the control plane, which would contain a list of tenant generation numbers to validate.

The result of the request to the control plane may indicate that only some of the attachment
generations are fresh: if this is the case, then the pageserver must selectively drop the
deletions from a stale generation, but still execute the deletions for attachments
that had a fresh generation.

The only correctness requirement for deletions is that they are _not_ executed prior
to the barrier in step 3. It is safe to delay a deletion, or indeed to never execute
it at all, if a node restarts while an in-memory queue of deletions is pending. Minimizing
leaking objects is an optimization, accomplished by making this queue persistent: see
[Persistent Deletion Queue](#persistent-deletion-queue) in the optimizations section.

### Deletion Part 2: WAL trim changes (delay `remote_consistent_lsn` updates)

Remote objects are not the only kind of deletion the pageserver does: it also indirectly deletes
WAL data, by feeding back remote_consistent_lsn to safekeepers, as a signal to the safekeepers that
they may drop data below this LSN.

We may solve for safety of remote_consistent_lsn updates in the same way as for S3 deletions. Before indicating to safekeepers
that they may trim up to some LSN `L0`, pageservers must do the following in order:

1. persist index_part that covers data up to LSN `L0`
2. call to control plane to validate their attachment + node generation number
3. update the `remote_consistent_lsn` that they send to the safekeepers to `L0`

**Note:** at step 3 we are not advertising the _latest_ remote_consistent_lsn, we are
advertising the value immediately before we started the validation RPC. This provides
a strong ordering guarantee.

Internally, the pagekeeper will have two remote_consistent_lsn values: the one that
reflects its latest write to remote storage, and the one that reflects the most
recent validation of generation number. It is only the latter value that may
be advertised to the outside world (i.e. to the safekeeper).

The control plane remains unaware of `remote_consistent_lsn`: it only has to validate
the freshness of generation numbers, thereby granting the pageserver permission to
share the information with the safekeeper.

For convenience, in subsequent sections and RFCs we will use "deletion" to mean both deletion
of objects in S3, and updates to the `remote_consistent_lsn`, as updates to the remote consistent
LSN are de-facto deletions done via the safekeeper, and both kinds of deletion are subject to
the same generation validation requirement.

### Index changes

Since object keys now include a generation suffix, the index of these keys must also be updated.

IndexPart currently stores keys and LSNs sufficient to reconstruct key names: this would be
extended to store the suffix as well.

This will increase the size of the file, but only modestly: layers are already encoded as
their string-ized form, so the overhead is about 20 bytes per layer. This will be less if/when
the index storage format is migrated to a binary format from JSON.

### Pageserver startup changes

- The pageserver must obtain a generation number by some means (see Control Plane Changes below) before
  doing any remote writes.
- The pageserver _may_ also do some synchronization of its attachments to see which are still
  valid, see "Synchronizing attachments on pageserver startup" in the optimizations section.

### `pageserver::Tenant` startup changes

#### Metadata reconciliation

Whether for a new attachment or just a pageserver restart, Tenant _should_
reconcile with remote metadata. Note that "reconcile" in this context is
used generically, and does not refer specifically to the logic currently
in the `reconcile_with_remote` function.

If the remote index reaches a lower LSN than the pageserver's local state,
then the pageserver is free to ignore the remote index and use its local
state as-is.

If the remote index reaches a higher LSN than the pageserver's local state,
then this indicates that the local state may not be safely used, unless the
safekeepers still have the WAL that spans the range between the local state
and the remote state. In this case, the pageserver may choose between replaying
the WAL to bring its local state up to date, or reconciling its local state with
the contents of S3 without replaying the WAL again:

- Option 1: Always reconcile with S3 when a tenant starts up
- Option 2: Prefer to recover by ingesting WAL, only do recovery from S3 if
  some needed LSN region is not available from the safekeeper (because some
  other node already deleted it by advertising its `remote_consistent_lsn`).

Option 1 is preferred over option 2, because:

- Option 1 is simpler to implement and test, since there is one code path always taken, rather than a conditional behavior.
- If there are already image layers
  in S3, it is cheaper to download them than to re-calculate them based on the WAL.
- Option 1 places less load on the safekeepers (a few nodes) and more on S3 (a
  very high scale service).
- Option 2 will leak objects referenced by the old pageserver's metadata, if the
  new pageserver is independently replaying based on their local state and generating
  alternative remote objects to represent the same data. This creates more work
  for scrub to clean up later.
- Option 2 will wastefully re-upload data that has already been uploaded to remote
  storage by an earlier node that already consumed the WAL.
- In option 2, then the `remote_consistent_lsn` could go _backwards_ from the
  perspective of the rest of the system, when tenant attachment is moved and some
  new pageserver starts from an earlier LSN than the previous pageserver had already
  advertised as being remote consistent. The rest of the system could handle this
  in principle, but it's rather an odd behavior, and ideally
  other components shouldn't have to handle this case.

There are scenarios where reconciling with remote storage imposes a longer startup
time for a tenant, if it happens to have some very fresh local state but is quite
far behind the remote state: it will have to download substantial data from S3
to serve the same LSNs that it could have served from local delta layers. The solution
to this will be a "warm secondary location" mode that pre-downloads data from S3,
which is described in a subsequent RFC.

#### Finding the remote index

Because index files are now suffixed with generation numbers, some changes are needed
to load them:

- In a clean migration of a tenant, the control plane may remember the tenant's most recent
  generation numbers, and provide them to the new node in the attach request. This is sufficient
  for the new node to directly GET the latest index.
- As an alternative optimization, we may use the storage broker for this (see [Publishing generation numbers ot storage brokers](#publishing-generation-numbers-to-storage-broker)). If we get the generation information
  this way, we must check that the old attachment generation is exactly 1 less than the current attachment, or if the attachment is the same but the node generation is exactly 1 less and the node ID is the same. Otherwise we must fall back to listing indices.
- As a fallback, newly attached tenants may issue a ListObjectsv2 request using index_part as a prefix to enumerate the indices and pick the most recent one by attachment generation (or node generation within attachment generation). The listing would typically only return 1-2 indices, assuming that we have promptly cleaned up old ones (see next section).

The tenant should never load an index with an attachment generation _newer_ than its own: tenants
are allowed to be attached with stale attachment generations during a multiply-attached
phase in a migration, and in this instance if the old location's pageserver restarts,
it should not try and load the newer generation's index.

#### Cleaning up previous generations' remote indices

Deletion of old indices is not necessary for correctness, although it is necessary
to avoid the ListObjects fallback in the previous section becoming ever more expensive.

Once the new attachment has written out its index_part.json, it may asynchronously clean up historic index_part.json
objects that were found, unless the control plane has indicated to us that the tenant is multiply attached
(see the subsequent HA RFC for this concept).

Deletion of historic index_part.json files doesn't necessarily have to go through
the deletion queue (it is always safe to drop these files once a more recent generation
has written its index), but it is beneficial to delay the deletions, for the benefit of
other nodes trying to read from the previous generation's data (e.g. some future read
replica feature) to delay these deletions, so the deletion queue should be used anyway.

We may choose to implement this deletion either as an explicit step after we
write out index_part for the first time in a pageserver's lifetime, or for
simplicity just do it periodically as part of the background scrub (see [scrubbing](#cleaning-up-orphan-objects-scrubbing));

### Control Plane Changes

#### Store generations for attaching tenants

- The `Project` table must store an attachment generation number for use when
  attaching the tenant to a new pageserver.
- The `/v1/tenant/:tenant_id/attach` pageserver API will require a generation number,
  which the control plane can supply by simply incrementing the `Project`'s attachment
  generation number each time the tenant is attached to a different server: the same database
  transaction that changes the assigned pageserver should also change the attachment generation.

#### The Generation API

This section describes an API that could be provided directly by the control plane,
or built as a separate microservice. In earlier parts of the RFC, when we
discuss the control plane providing generation numbers, we are referring to this API.

The API endpoints used by the pageserver to acquire and validate generation
numbers are quite simple, and only require access to some persistent and
linerizable storage (such as a database).

Building this into the control plane
is proposed as a least-effort option to exploit existing infrastructure and enable
updating attachment generations in the same transaction as updating
the `Project` itself, but it is not mandatory: this "Generation API" could
be built as a microservice. In practice, we will write such a miniature service
anyway, to enable E2E pageserver/compute testing without control plane.

The endpoints required by pageservers are:

##### `/register/node`

- Request: `{'node_id': <id>, 'metadata': {...}}`
- Response:
  - 200: `{'node_generation': <gen>}`
  - 404: unknown node_id
  - (Future: 429: flapping detected, nodes are fighting for the same node ID)
- Purpose: issue each pageserver process in a particular node_id with a unique generation number
- Server behavior: on each call, node generation number is persistently incremented
  before sending a response, concurrent calls are linearized such that all responses
  for a given node_id get a unique generation.
- Client behavior: client will not do any writes to S3 until it receives a successful response. On a 4xx response,
  client will terminate: this indicates to the client that is is misconfigured.

##### `/validate`

- Request: `{'node_id': <id>, 'node_gen': <gen>, 'tenants': [{tenant: <tenant id>, attach_gen: <gen>}, ...]}'`
- Response:
  - 200 `{'node_status': <bool>, 'tenants': [{tenant: <tenant id>, status: <bool>}...]}`
  - 404: unknown node_id
  - (On unknown tenants, omit tenant from `tenants` array)
- Purpose: enable the pageserver to discover whether the generations it holds are still current
- Server behavior: this is a read-only operation: simply compare the generations in the request with
  the generations known to the server, and set status to `true` if they match.
- Client behavior: clients must not do deletions within a tenant's remote data until they have
  received a response indicating the generation they hold for the tenant is current.

If the above endpoints are implemented by the control plane, then just the two endpoints are required, as
the control plane may advance attachment generation numbers directly in the database. However, if the
generation API was implement in a standalone service, then that service would also expose:

##### `/fence/tenant` (not needed if the above endpoints are built into control plane)

- Request: `{'tenant_id': <id>, 'attach_gen': <gen>}`
- Response: 200: `{'attach_gen': <gen> }`
  - (on unknown tenant_id, intitialize attach_gen to 1)
- Purpose: enable the control plane to safely attach a tenant to a new pageserver, without
  being certain that the previous pageserver has detached or stopped.
- Server behavior: increment latest attach gen by 1 and return new attach_gen
- Client behavior: the next attachment of the tenant should use the returned attach_gen. It is forbidden
  to attach a tenant to a different pageserver without calling this API, unless the old pageserver has
  return 200 to a detach request. In practice this API should be called between all attachment changes.

For use in automated tests of the pageserver, a stub implementation would provide all three endpoints.

### Timeline/Branch creation

All of the previous arguments for safety have described operations within
a timeline, where we may describe a sequence that includes updates to
index_part.json, and where reads and writes are coming from a postgres
endpoint (writes via the safekeeper).

Creating or destroying timeline is a bit different, because writes
are coming from the control plane.

We must be safe against scenarios such as:

- A tenant is attached to pageserver B while pageserver A is
  in the middle of servicing an RPC from the control plane to
  create or delete a tenant.
- A pageserver A has been sent a timeline creation request
  but becomes unresponsive. The tenant is attached to a
  different pageserver B, and the timeline creation request
  is sent there too.

To properly complete a timeline create/delete request, we must
be sure _after_ the pageserver write to remote storage, that its
generation number is still up to date:

- The pageserver can do this by calling into the control plane
  before responding to a request.
- Alternatively, the pageserver can include its generation in
  the response, and let the control plane validate that this
  generation is still current after receiving the response.

If some very slow node tries to do a timeline creation _after_
a more recent generation node has already created the timeline
and written some data into it, that must not cause harm. This
is provided in timeline creations by the way all the objects
within the timeline's remote path include a generation ID:
a slow node in an old generation that attempts to "create" a timeline
that already exists will just emit an index_part.json with
an old generation suffix.

Timeline IDs are never reused, so we don't have
to worry about the case of create/delete/create cycles.

**During timeline/tenant deletion, the control plane must not regard an operation
as complete when it receives a `202 Accepted` response**, because the node
that sent that response might become permanently unavailable. The control
plane must wait for the deletion to be truly complete (e.g. by polling
for 404 while the tenant is still attached to the same pageserver), and
handle the case where the pageserver becomes unavailable, either by waiting
for a replacement with the same node_id, or by re-attaching the tenant elsewhere.

**Sending a tenant/timeline deletion to a stale pageserver will still result
in deletion** -- the control plane must persist its intent to delete
a timeline/tenant before issuing any RPCs, and then once it starts, it must
keep retrying until the tenant/timeline is gone. This is already handled
by using a persistent `Operation` record that is retried indefinitely.

Tenant/timeline deletion operations don't necessarily have to go through
the deletion queue, as they are not subject to generation gating: once a
delete is issued by the control plane, it is a promise that the
control plane will keep trying until the deletion is done, so even stale
pageservers are permitted to go ahead and delete the objects. However,
using the deletion queue will still make sense in practice so that the
pageserver can use the same logic for coalescing deletions in DeleteObjects
requests.

Timeline deletion may result in a special kind of object leak, where
the latest generation attachment completes a deletion (including erasing
all objects in the timeline path), but some slow/partitioned node is
writing into the timeline path with a stale generation number. This would
not be caught by any per-timeline scrubbing (see [scrubbing](#cleaning-up-orphan-objects-scrubbing)), since scrubbing happens on the
attached pageserver, and once the timeline is deleted it isn't attached anywhere.
This scenario should be pretty rare, and the control plane can make it even
rarer by ensuring that if a tenant is in a multi-attached state (e.g. during
migration), we wait for that to complete before processing the deletion. Beyond
that, we may implement some other top-level scrub of timelines in
an external tool, to identify any tenant/timeline paths that are not found
in the control plane database.

Examples:

- Deletion, node restarts partway through:
  - By the time we returned 204, we have written a remote delete marker
  - Any subsequent incarnation of the same node_id will see the remote
    delete marker and continue to process the deletion
  - We only require that the control plane does not change the tenant's
    attachment while waiting for the deletion.
  - If the original pageserver is lost permanently and no replacement
    with the same node_id is available, then the control plane must recover
    by re-attaching the tenant to a different node.
- Creation, node becomes unresponsive partway through.
  - Control plane will see HTTP request timeout, keep re-issuing
    request to whoever is the latest attachment point for the tenant
    until it succeeds.
  - Stale nodes may be trying to execute timeline creation: they will
    write out index_part.json files with stale node generation or
    stale attachment generation: these will be eventually cleaned up
    by the same mechanism as other old indices.

## Implementation Part 2: Optimizations

### Persistent deletion queue

Between writing our a new index_part.json that doesn't reference an object,
and executing the deletion, an object passes through a window where it is
only referenced in memory, and could be leaked if the pageserver is stopped
uncleanly. That introduces conflicting incentives: we would like to delay
deletions to minimize control plane load and allow aggregation of full-sized
DeleteObjects requests, but we would also like to minimize leakage by executing
deletions promptly.

To resolve this, we may make the deletion queue persistent, writing out
deletion lists as S3 objects for later execution. This shrinks the window
of possible object loss to the gap between writing index_part.json, and
writing the next deletion list ot S3. The actual deletion of objects
(with its requirement to sync with the control plane) may happen much
later.

The flow of deletion becomes:

1. Enqueue in memory
2. Enqueue persistently by writing out a deletion list, storing the
   attachment generations and node generation.
3. Validate the deletion list by calling to the control plane
4. Execute the valid parts of the deletion list (i.e. call DeleteObjects)

There is existing work (https://github.com/neondatabase/neon/pull/4960) to
create a deletion queue: this would be extended by adding the "step 3" validation
step.

Since this whole section is an optimization, there is a lot of flexibility
in exactly how the deletion queue should work, especially in the timing
of the validation step:

- Option A (simplest):validate the
  deletion list before persisting it. This has the downside of delaying
  persistence of deletes until the control plane is available.
- Option B: validate a list at the point of executing it. This has the downside
  that because we're doing the validation much later, there is a good chance
  some attachments might have changed, and we will end up leaking objects.
- Option C (preferred): validate lazily in the background after persistence of the list, maintaining
  an "executable" pointer into the list of deletion lists to reflect
  which ones are elegible for execution, and re-writing deletion lists if
  they are found to contain some operations not elegible for execution. This
  is the most efficient approach in terms of I/O, at the cost of some complexity
  in implementation.

As well as reducing leakage, the ability of a persistent queue to accumulate
deletions over long timescale has side benefits:

- Over enough time we will always accumulate enough keys to issue full-sized
  (1000 object) DeleteObjects requests, minimizing overall request count.
- If in future we implement a read-only mode for pageservers to read the
  data of tenants, then delaying deletions avoids the need for that remote
  pageserver to be fully up to date with the latest index when serving reads
  from old LSNs. The read-only node might be using stale metadata that refers
  to objects eliminated in a recent compaction, but can still service reads
  without refreshing metadata as long as the old objects deletion is delayed.

#### Deletion queue persistence format

Persistence is to S3 rather than local disk so that if a pageserver
is stopped uncleanly and then the same node_id is started on a fresh
physical machine, the deletion queue can still continue without leaking
objects.

The persisted queue is broken up into a series of lists, written out
once some threshold number of deletions are accumulated. This should
be tuned to target an object size of ~1MB to avoid the expense of
writing and reading many tiny objects.

Each deletion list has a sequence number: this records the logical
ordering of the lists, so that we may use sequence numbers to succinctly
store knowledge about up to which point the deletions have been validated.

In addition to the lists themselves, a header object would be used
to store state about how far validation has proceeded (in the "Option C" case
above, for background validation), and to record the next sequence number in
case there are no deletion lists at present.

Deletion queue objects will be stored outside the `tenants/` path, and
with the node generation ID in the name. The paths would look something like:

```
  # Deletion List
  deletion/<node id>/<sequence>-<generation>.list

  # Header object, st
  deletion/<node id>/header-<generation>
```

#### Deletion queue replay on startup

When starting up with a new generation number, a pageserver should avoid
leaking objects by ingesting the deletion queue from its previous lifetime, but
ignore any un-validated content:

1. List all objects in `deletion/<node id>`. Load the header.
2. Drop any that are not below the validated horizon in the header
3. Write a new header in the current generation number
4. Process the validated lists from the previous generation in the same
   way as any lists that would be generated within this generation.

The number of objects leaked in this process depends on how frequently we
do validation during normal operations, and whether the previous pageserver
instance was terminated cleanly and validated its lists in the process.

### Synchronizing attachments on pageserver startup

For correctness, it is not necessary for a node to synchronize its attachments on startup: it
may continue to ingest+serve tenants on stale attachment generation numbers harmlessly.

As an optimization, we may avoid doing spurious S3 writes within a stale generation,
by using the same generation-checking API that is used for deletions. On startup,
concurrently with loading state from disk, the pageserver may issue RPCs to
the control plane to discover if any of its attachments are stale.

If an attachment is stale, then the pageserver will not do any S3 writes. However,
the attachment will still ingest the WAL and serve reads: this is necessary
for high availability, as some endpoint might still be using this
node for reads. To avoid overwhelming local disk with data that cannot be
offloaded to remote storage, we may impose some time/space threshold on
the attachment when operating in this mode: when exceeded, the attachment
would go into Broken state. It is the responsibility of the control plane
to ensure that endpoints are using the latest attachment location before this
happens.

In principle we could avoid the need to exchange O(attachment_count) information
at startup by having the control plane keep track of whether any changes
happened that would affect the pageserver's attachments while it was unavailable,
but this would impose complexity on the control plane code to track such
dirty/clean state. It is simpler to just check all the attachments, and
relatively inexpensive since validating generation numbers is a read-only
request to the control plane.

### Cleaning up orphan objects (scrubbing)

An orphan object is any object which is no longer referenced by a running node or by metadata.

Examples of how orphan objects arise:

- A node is doing compaction and writes a layer object, then crashes before it writes the
  index_part.json that references that layer.
- A partition node carries on running for some time, and writes out an unbounded number of
  objects while it believes itself to be the rightful writer for a tenant.

Orphan objects are functionally harmless, but have a small cost due to S3 capacity consumed. We
may clean them up at some time in the future, but doing a ListObjectsv2 operation and cross
referencing with the latest metadata to identify objects which are not referenced.

Scrubbing will be done only by an attached pageserver (not some third party process), and deletions requested during scrub will go through the same
validation as all other deletions: the attachment generation must be
fresh. This avoids the possibility of a stale pageserver incorrectly
thinking than an object written by a newer generation is stale, and deleting
it.

### Publishing generation numbers to storage broker

The storage broker acts as an ephemeral store of some per-timeline
metadata, updated by the safekeeper.

Generation numbers may be passed from pageserver back to safekeeper
in the feedback messages on wal connections and stored in the storage
broker by the safekeeper. This is the same data path already used
for `remote_consistent_lsn` updates.

Nothing we publish to the storage broker is for use in correctness, just
certain optimizations like:

- Discovering the latest generation's index without doing a ListObjects
  request
- Retaining safekeeper knowledge of which pageserver generations are
  stale across restarts.
- In future, for remote nodes doing passive reads of historical LSNs from
  S3 to notice when the current generation for a tenant changes.

### Safekeeper optimization for stale pageservers

As an optimization, we may extend the safekeeper to be asynchronously updated about pageserver node
generation numbers. We may do this by including the generation in messages from pageserver to safekeeper,
and having the safekeeper write the highest generation number it has seen for each node to the storage broker.

Once the safekeeper has visibility of the most recent generation, it may reject requests from pageservers
with stale node generation numbers: this would reduce any possible extra load on the safekeeper from stale pageservers,
and provide feedback to stale pageservers that they should shut down.

This is not required for safety: reads from a stale pageserver are functionally harmless and only
waste system resources. Logical writes (i.e. updates to remote_persistent_lsn that can cause trimming)
are handled on the pageserver side (see "WAL trim changes" above).

Note that the safekeeper should only reject reads for stale _pageserver_ generations. Stale _attachment_
generations are valid for reads, as a tenant may be multi-attached during a migration, where two different
pageservers are both replaying the WALs for the same tenant.

## Operational impact

### Availability

Coordination of generation numbers via the control plane introduce a dependency for certain
operations:

1. Starting new pageservers (or activating pageservers after a restart)
2. Executing enqueued deletions
3. Advertising updated `remote_consistent_lsn` to enable WAL trimming

Item 1. would mean that some in-place restarts that previously would have resumed service even if the control plane were
unavailable, will now not resume service to users until the control plane is available. We could
avoid this by having a timeout on communication with the control plane, and after some timeout,
resume service with the node's previous generation (assuming this was persisted to disk). However,
this is unlikely to be needed as the control plane is already an essential & highly available component. Also, having a node re-use an old generation number would complicate
reasoning about the system, as it would break the invariant that one (node_id, generation)
tuple uniquely identifies one process lifetime -- it is not recommended to implement this.

Item 2. is a non-issue operationally: it's harmless to delay deletions, the only impact of objects pending deletion is
the S3 capacity cost.

Item 3. is an issue if safekeepers are low on disk space and the control plane is unavailable for a long time.

For a managed service, the general approach should be to make sure we are monitoring & respond fast enough
that control plane outages are bounded in time. The separation of console and control plane will also help
to keep the control plane itself simple and robust.

We should also implement an "escape hatch" config for node generation numbers, where in a major disaster outage,
we may manually run pageservers with a hand-selected generation number, so that we can bring them online
independently of a control plane.

### Rollout

Although there is coupling between components, we may deploy most of the new data plane components
independently of the control plane: initially they can just use a static generation number.

#### Phase 1

The pageserver is deployed with some special config to:

- Always act like everything is generation 1 and do not wait for a control plane issued generation on startup.
- Skip the places in deletion and remote_consistent_lsn updates where we would call into control plane

The storage broker will tolerate the timeline state omitting generation numbers (only
relevant if we implement [publishing generation numbers to the storage broker](#publishing-generation-numbers-to-storage-broker).

The safekeeper will be aware of both new and old versions of `PageserverFeedback` message, and tolerate
the old version.

#### Phase 2

The control plane changes are deployed: control plane will now track and increment generation numbers.

#### Phase 3

The pageserver is deployed with its control-plane-dependent changes enabled: it will now require
the control plane to issue a generation number, and require to communicate with the control plane
prior to processing deletions.

### On-disk backward compatibility

Backward compatibility with existing data is straightforward:

- When reading the index, we may assume that any layer whose metadata doesn't include
  generations will have a generation-less key path.
- When locating the index file on attachment, we may use the "fallback" listing path
  and if there is only a generation-less index, that is the one we load.

It is not necessary to re-write existing layers: even new index files will be able
to represent generation-less layers.

### On-disk forward compatibility

We will do a two phase rollout, probably over multiple releases because we will naturally
have some of the read-side code ready before the overall functionality is ready:

1. Deploy pageservers which understand the new index format and generation suffixes
   in keys, but do not write objects with generation numbers in the keys.
2. Deploy pageservers that write objects with generation numbers in the keys.

Old pageservers will be oblivious to generation numbers. That means that they can't
read objects with generation numbers in the name. This is why we must
first step must deploy the ability to read, before the second step
starts writing them.

# Appendix A: Examples of use in high availability/failover

The generation numbers proposed in this RFC are adaptable to a variety of different
failover scenarios and models. The sections below sketch how they would work in practice.

### Fast restart of a pageserver

"fast" here means that the restart is done before any other element in the system
has taken action in response to the node being down.

- After restart, the node does no writes until it can obtain a fresh generation
  number from the control plane.
- Once it has a generation number, it may activate all existing attachments. The
  generation of its attachments is stored on disk. This may be stale, but that is
  safe.
- If any of its attachments were in fact stale (i.e. had be reassigned to another
  node while this node was offline), then:
  - Deletions to those attachments will be blocked by generation validation on the
    delete path
  - The control plane is expected to eventually detach this tenant from the
    pageserver.

### Failure of a pageserver

In this context, read "failure" as the most ambiguous possible case, where
a pageserver is unavailable to clients and control plane, but may still be executing and talking
to S3.

#### Case A: re-attachment to other nodes

1. Let's say node 0 fails in a cluster of three nodes 0, 1, 2.
2. Some external mechanism notices that the node is unavailable and initiates
   movement of all tenants attached to that node to a different node. In
   this example it would mean incrementing the attachment generation
   of all tenants that were attached to node 0, and attaching them
   to node 1 or 2 based on some distribution rule (this might be round
   robin, or capacity based).
3. A tenant which is now attached to node 1 will _also_ still be attached to node
   0, from the perspective of node 0. Let's say it was originally attached with
   generation suffix 00000001-0000-00000001 -- its new suffix on node 1
   might be 00000002-0001-00000001 (attachment gen 2, on node 1).
4. S3 writes will continue from nodes 0 and 1: there will be an index_part.json-00000001-0000-00000001
   \_and\* an index_part.json-00000002-0001-00000001. Objects written under the old suffix
   after the new attachment was created do not matter from the rest of the system's
   perspective: the endpoints are reading from the new attachment location. Objects
   written by node 0 are just garbage that can be cleaned up at leisure. Node 0 will
   not do any deletions because it can't synchronize with control plane, or if it could, it would be informed that is no longer the most recent generation and hence not do the garbage collection.

Eventually, node 0 will somehow realize it should stop running by some means, although
this is not necessary for correctness:

- A hard-stop of the VM it is running on
- It tries to communicate with another peer that sends it an error response
  indicating its node generation is out of date
- It tries to do some deletions, and discovers when synchronizing with the
  control plane that its node generation number is stale.

#### Case B: direct node replacement with same node_id

This is the scenario we would experience if running a kubernetes Statefulset
of pageservers: kubernetes would start a new node without guaranteeing that the
old "failed" node is really dead.

1. Let's say node 0 fails, and there may be some other peers but they aren't relevant.
2. Some external mechanism notices that the node is unavailable, and creates
   a "new node 0" (Node 0b) which is a physically separate server. The original node 0
   (Node 0a) may still be running.
3. On startup, node 0b acquires a new generation number. It doesn't have any local state,
   so the control plane must send attach requests to node 0b for all the tenants. However,
   these do not have to increment the attachment generation as they're still attached to
   logical node 0, it's just a different physical node. So we have an O(tenant_count) communication
   to the pageserver, but not an O(tenant_count) write to the database.
4. S3 writes continue from nodes 0a and 0b, but the writes do not collide due to different
   node generation in the suffix, and the writes from node 0a are not visible to the rest
   of the system because endpoints are reading only from node 0b.

# Appendix B: interoperability with other features

## Sharded Keyspace

The design in this RFC maps neatly to a sharded keyspace design where subsets of the key space
for a tenant are assigned to different pageservers:

- the "unit of work" for attachments becomes something like a TenantShard rather than a Tenant
- TenantShards get generation numbers just as Tenants do.
- Write workload (ingest, compaction) for a tenant is spread out across pageservers via
  TenantShards, but each TenantShard still has exactly one valid writer at a time.

## Read replicas

For historical reads to LSNs below the remote persistent LSN, any node may act as a reader at any
time: remote data is logically immutable data, and the use of deferred deletion in this RFC helps
mitigate the fact that remote data is not _physically_ immutable (i.e. the actual data for a given
page moves around as compaction happens).

A read replica needs to be aware of generations in remote data in order to read the latest
metadata (find the index_part.json with the latest suffix). It may either query this
from the control plane, or more efficiently read it from the storage broker (see [storage broker section in optimizations](#publishing-generation-numbers-to-storage-broker)).

## Seamless migration

To make tenant migration totally seamless, we will probably want to intentionally double-attach
a tenant briefly, serving reads from the old node while waiting for the new node to be ready.

This RFC enables that double-attachment: two nodes may be attached at the same time, with the migration destination
having a higher generation number. The old node will be able to ingest and serve reads, but not
do any deletes. The new node's attachment must also avoid doing deletes, a new piece of state
will be needed for this in the control plane's definition of an attachment.

## Warm secondary locations

To enable faster tenant movement after a pageserver is lost, we will probably want to spend some
disk capacity on keeping standby locations populated with local disk data.

There's no conflict between this RFC and that: implementing warm secondary locations on a per-tenant basis
would be a separate change to the control plane to store standby location(s) for a tenant. Because
the standbys do not write to S3, they do not need to be assigned generation IDs. When a tenant is
re-attached to a standby location, that would increment the tenant attachment generation and this
would work the same as any other attachment change, but with a warm cache.

## Ephemeral node IDs

This RFC intentionally avoids changing anything fundamental about how pageservers are identified
and registered with the control plane, to avoid coupling the implementation of pageserver split
brain protection with more fundamental changes in the management of the pageservers.

However, we also accommodate the possibility of a future change to fully ephemeral pageserver IDs,
where an attachment would implicitly have a lifetime bounded to one pageserver process lifetime,
as the pageserver ID would change on restart. In this model, separate node and attachment
generations are unnecessary, but the storage format doesn't change: the [generation suffix](#generation-suffix)
may simply be the attachment generation, without any per-node component, as the attachment
generation would change any time an attached node restarted.

That is just one possible future direction for node management: the imporant thing is just that
we avoid coding in assumptions about cluster management into our storage format, hence the opaque
u64 used in the object suffix.
