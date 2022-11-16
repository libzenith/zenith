/*-------------------------------------------------------------------------
 *
 * rwset.h
 *
 * contrib/remotexact/rwset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RWSET_H
#define RWSET_H

#include "postgres.h"

#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/itemptr.h"
#include "utils/snapshot.h"

/*
 * Header of the read/write set
 */
typedef struct RWSetHeader
{
	Oid			dbid;
	uint64 		region_set;
} RWSetHeader;

/*
 * A page in the read set
 */
typedef struct RWSetPage
{
	BlockNumber blkno;

	dlist_node	node;
} RWSetPage;

/*
 * A tuple in the read set
 */
typedef struct RWSetTuple
{
	ItemPointerData tid;

	dlist_node	node;
} RWSetTuple;

/*
 * A relation in the read set
 */
typedef struct RWSetRelation
{
	bool		is_index;
	Oid			relid;
	int8		region;
	XidCSN		csn;
	bool		is_table_scan;
	dlist_head	pages;
	dlist_head	tuples;

	dlist_node	node;
} RWSetRelation;

/*
 * A read/write set
 */
typedef struct RWSet
{
	MemoryContext context;
	RWSetHeader header;
	dlist_head	relations;
	char	   *writes;
	int			writes_len;
} RWSet;

extern RWSet *RWSetAllocate(void);
extern void RWSetFree(RWSet *rwset);
extern void RWSetDecode(RWSet *rwset, StringInfo msg);
extern char *RWSetToString(RWSet *rwset);

#endif							/* RWSET_H */
