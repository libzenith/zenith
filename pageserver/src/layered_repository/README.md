# Overview

The on-disk format is based on immutable files. The page server
receives a stream of incoming WAL, parses the WAL records to determine
which pages they apply to, and accumulates the incoming changes in
memory. Every now and then, the accumulated changes are written out to
new immutable files. This process is called checkpointing. Old versions
of on-disk files that are not needed by any timeline are removed by GC
process.

# Terms used in layered repository

- Relish - one PostgreSQL relation or similarly treated file.
- Segment - one slice of a Relish that is stored in a LayeredTimeline.
- Layer -  specific version of a relish Segment in a range of LSNs.

Layers can be InMemory or OnDisk:
- InMemory layer is not durably stored and needs to rebuild from WAL on pageserver start.
- OnDisk layer is durably stored.

OnDisk layers can be Image or Delta:
- ImageLayer represents an image or a snapshot of a segment at one particular LSN.
- DeltaLayer represents a collection of WAL records or page images in a range of LSNs.

Dropped segments are always represented on disk by DeltaLayer.

LSN range defined by start_lsn and end_lsn:
- start_lsn is inclusive.
- end_lsn is exclusive.

For an open in-memory layer, the end_lsn is MAX_LSN. For a frozen
in-memory layer or a delta layer, it is a valid end bound. An image
layer represents snapshot at one LSN, so end_lsn is always the
snapshot LSN + 1

Layers can be open or historical:
- Open layer is a writeable one. Only InMemory layer can be open.
FIXME: If open layer is dropped, it is not writeable, so it should be turned into historical,
but now it is not implemented - see bug #569.
- Historical layer is the one that cannot be modified anymore. Now only OnDisk layers can be historical.

- LayerMap - a map that tracks what layers exist for all the relishes in a timeline.

LayerMap consists of two data structures:
- segs - All the layers keyed by segment tag
- open_layers - data structure that hold all open layers ordered by oldest_pending_lsn for quick access during checkpointing. oldest_pending_lsn is the LSN of the oldest page version stored in this layer.

All operations that update InMemory Layers should update both structures to keep them up-to-date.

- LayeredTimeline - implements Timeline interface.

All methods of LayeredTimeline are aware of its ancestors and return data taking them into account.
TODO: Are there any exceptions to this?
For example, timeline.list_rels(lsn) will return all segments that are visible in this timeline at the LSN,
including ones that were not modified in this timeline and thus don't have a layer in the timeline's LayerMap.

TODO:
Describe GC and checkpoint interval settings.

# Layer files (On-disk layers)

The files are called "layer files". Each layer file corresponds
to one RELISH_SEG_SIZE slice of a PostgreSQL relation fork or
non-rel file in a range of LSNs. The layer files
for each timeline are stored in the timeline's subdirectory under
.zenith/tenants/<tenantid>/timelines.

There are two kind of layer file: base images, and deltas. A base
image file contains a layer of a segment as it was at one LSN,
whereas a delta file contains modifications to a segment - mostly in
the form of WAL records - in a range of LSN

base image file:

    rel_<spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<start LSN>

delta file:

    rel_<spcnode>_<dbnode>_<relnode>_<forknum>_<segno>_<start LSN>_<end LSN>

For example:

    rel_1663_13990_2609_0_10_000000000169C348
    rel_1663_13990_2609_0_10_000000000169C348_0000000001702000

In addition to the relations, with "rel_*" prefix, we use the same
format for storing various smaller files from the PostgreSQL data
directory. They will use different suffixes and the naming scheme up
to the LSNs vary. The Zenith source code uses the term "relish" to
mean "a relation, or other file that's treated like a relation in the
storage" For example, a base image of a CLOG segment would be named
like this:

    pg_xact_0000_0_00000000198B06B0

There is no difference in how the relation and non-relation files are
managed, except that the first part of file names is different.
Internally, the relations and non-relation files that are managed in
the versioned store are together called "relishes".

If a file has been dropped, the last layer file for it is created
with the _DROPPED suffix, e.g.

    rel_1663_13990_2609_0_10_000000000169C348_0000000001702000_DROPPED


## Notation used in this document

The full path of a delta file looks like this:

    .zenith/tenants/941ddc8604413b88b3d208bddf90396c/timelines/4af489b06af8eed9e27a841775616962/rel_1663_13990_2609_0_10_000000000169C348_0000000001702000

For simplicity, the examples below use a simplified notation for the
paths.  The tenant ID is left out, the timeline ID is replaced with
the human-readable branch name, and spcnode+dbnode+relnode+forkum+segno
with a human-readable table name. The LSNs are also shorter. For
example, a base image file at LSN 100 and a delta file between 100-200
for 'orders' table on 'main' branch is represented like this:

    main/orders_100
    main/orders_100_200


# Creating layer files

Let's start with a simple example with a system that contains one
branch called 'main' and two tables, 'orders' and 'customers'. The end
of WAL is currently at LSN 250. In this starting situation, you would
have these files on disk:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/customers_100
	main/customers_100_200
	main/customers_200

In addition to those files, the recent changes between LSN 200 and the
end of WAL at 250 are kept in memory. If the page server crashes, the
latest records between 200-250 need to be re-read from the WAL.

Whenever enough WAL has been accumulated in memory, the page server
writes out the changes in memory into new layer files. This process
is called "checkpointing" (not to be confused with the PostgreSQL
checkpoints, that's a different thing). The page server only creates
layer files for relations that have been modified since the last
checkpoint. For example, if the current end of WAL is at LSN 450, and
the last checkpoint happened at LSN 400 but there hasn't been any
recent changes to 'customers' table, you would have these files on
disk:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_100
	main/customers_100_200
	main/customers_200

If the customers table is modified later, a new file is created for it
at the next checkpoint. The new file will cover the "gap" from the
last layer file, so the LSN ranges are always contiguous:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_100
	main/customers_100_200
	main/customers_200
	main/customers_200_500
	main/customers_500

## Reading page versions

Whenever a GetPage@LSN request comes in from the compute node, the
page server needs to reconstruct the requested page, as it was at the
requested LSN. To do that, the page server first checks the recent
in-memory layer; if the requested page version is found there, it can
be returned immediatedly without looking at the files on
disk. Otherwise the page server needs to locate the layer file that
contains the requested page version.

For example, if a request comes in for table 'orders' at LSN 250, the
page server would load the 'main/orders_200_300' file into memory, and
reconstruct and return the requested page from it, as it was at
LSN 250. Because the layer file consists of a full image of the
relation at the start LSN and the WAL, reconstructing the page
involves replaying any WAL records applicable to the page between LSNs
200-250, starting from the base image at LSN 200.

# Multiple branches

Imagine that a child branch is created at LSN 250:

            @250
    ----main--+-------------------------->
               \
                +---child-------------->


Then, the 'orders' table is updated differently on the 'main' and
'child' branches. You now have this situation on disk:

    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    main/orders_300
    main/orders_300_400
    main/orders_400
    main/customers_100
    main/customers_100_200
    main/customers_200
    child/orders_250_300
    child/orders_300
    child/orders_300_400
    child/orders_400

Because the 'customers' table hasn't been modified on the child
branch, there is no file for it there. If you request a page for it on
the 'child' branch, the page server will not find any layer file
for it in the 'child' directory, so it will recurse to look into the
parent 'main' branch instead.

From the 'child' branch's point of view, the history for each relation
is linear, and the request's LSN identifies unambiguously which file
you need to look at. For example, the history for the 'orders' table
on the 'main' branch consists of these files:

    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    main/orders_300
    main/orders_300_400
    main/orders_400

And from the 'child' branch's point of view, it consists of these
files:

    main/orders_100
    main/orders_100_200
    main/orders_200
    main/orders_200_300
    child/orders_250_300
    child/orders_300
    child/orders_300_400
    child/orders_400

The branch metadata includes the point where the child branch was
created, LSN 250. If a page request comes with LSN 275, we read the
page version from the 'child/orders_250_300' file. We might also
need to reconstruct the page version as it was at LSN 250, in order
to replay the WAL up to LSN 275, using 'main/orders_200_300' and
'main/orders_200'. The page versions between 250-300 in the
'main/orders_200_300' file are ignored when operating on the child
branch.

Note: It doesn't make any difference if the child branch is created
when the end of the main branch was at LSN 250, or later when the tip of
the main branch had already moved on. The latter case, creating a
branch at a historic LSN, is how we support PITR in Zenith.


# Garbage collection

In this scheme, we keep creating new layer files over time. We also
need a mechanism to remove old files that are no longer needed,
because disk space isn't infinite.

What files are still needed? Currently, the page server supports PITR
and branching from any branch at any LSN that is "recent enough" from
the tip of the branch.  "Recent enough" is defined as an LSN horizon,
which by default is 64 MB.  (See DEFAULT_GC_HORIZON). For this
example, let's assume that the LSN horizon is 150 units.

Let's look at the single branch scenario again. Imagine that the end
of the branch is LSN 525, so that the GC horizon is currently at
525-150 = 375

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/orders_400_500
	main/orders_500
	main/customers_100
	main/customers_100_200
	main/customers_200

We can remove the following files because the end LSNs of those files are
older than GC horizon 375, and there are more recent layer files for the
table:

	main/orders_100       DELETE
	main/orders_100_200   DELETE
	main/orders_200       DELETE
	main/orders_200_300   DELETE
	main/orders_300       STILL NEEDED BY orders_300_400
	main/orders_300_400   KEEP, NEWER THAN GC HORIZON
	main/orders_400       ..
	main/orders_400_500   ..
	main/orders_500       ..
	main/customers_100      DELETE
	main/customers_100_200  DELETE
	main/customers_200      KEEP, NO NEWER VERSION

'main/customers_100_200' is old enough, but it cannot be
removed because there is no newer layer file for the table.

Things get slightly more complicated with multiple branches. All of
the above still holds, but in addition to recent files we must also
retain older shapshot files that are still needed by child branches.
For example, if child branch is created at LSN 150, and the 'customers'
table is updated on the branch, you would have these files:

	main/orders_100        KEEP, NEEDED BY child BRANCH
	main/orders_100_200    KEEP, NEEDED BY child BRANCH
	main/orders_200        DELETE
	main/orders_200_300    DELETE
	main/orders_300        KEEP, NEWER THAN GC HORIZON
	main/orders_300_400    KEEP, NEWER THAN GC HORIZON
	main/orders_400        KEEP, NEWER THAN GC HORIZON
	main/orders_400_500    KEEP, NEWER THAN GC HORIZON
	main/orders_500        KEEP, NEWER THAN GC HORIZON
	main/customers_100       DELETE
	main/customers_100_200   DELETE
	main/customers_200       KEEP, NO NEWER VERSION
	child/customers_150_300  DELETE
	child/customers_300      KEEP, NO NEWER VERSION

In this situation, 'main/orders_100' and 'main/orders_100_200' cannot
be removed, even though they are older than the GC horizon, because
they are still needed by the child branch. 'main/orders_200'
and 'main/orders_200_300' can still be removed.

If 'orders' is modified later on the 'child' branch, we will create a
new base image and delta file for it on the child:

	main/orders_100
	main/orders_100_200

	main/orders_300
	main/orders_300_400
	main/orders_400
	main/orders_400_500
	main/orders_500
	main/customers_200
	child/customers_300
	child/orders_150_400
	child/orders_400

After this, the 'main/orders_100' and 'main/orders_100_200' file could
be removed. It is no longer needed by the child branch, because there
is a newer layer file there. TODO: This optimization hasn't been
implemented! The GC algorithm will currently keep the file on the
'main' branch anyway, for as long as the child branch exists.


# TODO: On LSN ranges

In principle, each relation can be checkpointed separately, i.e. the
LSN ranges of the files don't need to line up. So this would be legal:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_300_400
	main/orders_400
	main/customers_150
	main/customers_150_250
	main/customers_250
	main/customers_250_500
	main/customers_500

However, the code currently always checkpoints all relations together.
So that situation doesn't arise in practice.

It would also be OK to have overlapping LSN ranges for the same relation:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300
	main/orders_250_350
	main/orders_350
	main/orders_300_400
	main/orders_400

The code that reads the layer files should cope with this, but this
situation doesn't arise either, because the checkpointing code never
does that.  It could be useful, however, as a transient state when
garbage collecting around branch points, or explicit recovery
points. For example, if we start with this:

	main/orders_100
	main/orders_100_200
	main/orders_200
	main/orders_200_300
	main/orders_300

And there is a branch or explicit recovery point at LSN 150, we could
replace 'main/orders_100_200' with 'main/orders_150' to keep a
layer only at that exact point that's still needed, removing the
other page versions around it. But such compaction has not been
implemented yet.
