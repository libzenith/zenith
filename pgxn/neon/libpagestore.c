/*-------------------------------------------------------------------------
 *
 * libpagestore.c
 *	  Handles network communications with the remote pagestore.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	 contrib/neon/libpqpagestore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pagestore_client.h"
#include "fmgr.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "storage/buf_internals.h"

#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"

#include "neon.h"
#include "walproposer.h"
#include "walproposer_utils.h"

#define PageStoreTrace DEBUG5

#define MAX_RECONNECT_ATTEMPTS 5
#define RECONNECT_INTERVAL_USEC 1000000

bool		connected = false;
PGconn	   *pageserver_conn = NULL;

/*
 * WaitEventSet containing:
 * - WL_SOCKET_READABLE on pageserver_conn,
 * - WL_LATCH_SET on MyLatch, and
 * - WL_EXIT_ON_PM_DEATH.
 */
WaitEventSet *pageserver_conn_wes = NULL;

/* GUCs */
char	   *neon_timeline;
char	   *neon_tenant;
int32		max_cluster_size;
char	   *page_server_connstring;
char	   *neon_auth_token;

int			n_unflushed_requests = 0;
int			flush_every_n_requests = 8;
int			readahead_buffer_size = 128;

bool	(*old_redo_read_buffer_filter) (XLogReaderState *record, uint8 block_id) = NULL;

static void pageserver_flush(void);

static bool
pageserver_connect(int elevel)
{
	char	   *query;
	int			ret;
	const char *keywords[3];
	const char *values[3];
	int			n;

	Assert(!connected);

	/*
	 * Connect using the connection string we got from the
	 * neon.pageserver_connstring GUC. If the NEON_AUTH_TOKEN environment
	 * variable was set, use that as the password.
	 *
	 * The connection options are parsed in the order they're given, so
	 * when we set the password before the connection string, the
	 * connection string can override the password from the env variable.
	 * Seems useful, although we don't currently use that capability
	 * anywhere.
	 */
	n = 0;
	if (neon_auth_token)
	{
		keywords[n] = "password";
		values[n] = neon_auth_token;
		n++;
	}
	keywords[n] = "dbname";
	values[n] = page_server_connstring;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	pageserver_conn = PQconnectdbParams(keywords, values, 1);

	if (PQstatus(pageserver_conn) == CONNECTION_BAD)
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		PQfinish(pageserver_conn);
		pageserver_conn = NULL;

		ereport(elevel,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg(NEON_TAG "could not establish connection to pageserver"),
				 errdetail_internal("%s", msg)));
		return false;
	}

	query = psprintf("pagestream %s %s", neon_tenant, neon_timeline);
	ret = PQsendQuery(pageserver_conn, query);
	if (ret != 1)
	{
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		neon_log(elevel, "could not send pagestream command to pageserver");
		return false;
	}

	pageserver_conn_wes = CreateWaitEventSet(TopMemoryContext, 3);
	AddWaitEventToSet(pageserver_conn_wes, WL_LATCH_SET, PGINVALID_SOCKET,
			  MyLatch, NULL);
	AddWaitEventToSet(pageserver_conn_wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
			  NULL, NULL);
	AddWaitEventToSet(pageserver_conn_wes, WL_SOCKET_READABLE, PQsocket(pageserver_conn), NULL, NULL);

	while (PQisBusy(pageserver_conn))
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(pageserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

				PQfinish(pageserver_conn);
				pageserver_conn = NULL;
				FreeWaitEventSet(pageserver_conn_wes);
				pageserver_conn_wes = NULL;

				neon_log(elevel, "could not complete handshake with pageserver: %s",
						 msg);
				return false;
			}
		}
	}

	neon_log(LOG, "libpagestore: connected to '%s'", page_server_connstring);

	connected = true;
	return true;
}

/*
 * A wrapper around PQgetCopyData that checks for interrupts while sleeping.
 */
static int
call_PQgetCopyData(char **buffer)
{
	int			ret;

retry:
	ret = PQgetCopyData(pageserver_conn, buffer, 1 /* async */ );

	if (ret == 0)
	{
		WaitEvent	event;

		/* Sleep until there's something to do */
		(void) WaitEventSetWait(pageserver_conn_wes, -1L, &event, 1, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (event.events & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(pageserver_conn))
			{
				char	   *msg = pchomp(PQerrorMessage(pageserver_conn));
				neon_log(LOG, "could not get response from pageserver: %s", msg);
				pfree(msg);
				return -1;
			}
		}

		goto retry;
	}

	return ret;
}


static void
pageserver_disconnect(void)
{
	/*
	 * If anything goes wrong while we were sending a request, it's not clear
	 * what state the connection is in. For example, if we sent the request
	 * but didn't receive a response yet, we might receive the response some
	 * time later after we have already sent a new unrelated request. Close
	 * the connection to avoid getting confused.
	 */
	if (connected)
	{
		neon_log(LOG, "dropping connection to page server due to error");
		PQfinish(pageserver_conn);
		pageserver_conn = NULL;
		connected = false;

		prefetch_on_ps_disconnect();
	}
	if (pageserver_conn_wes != NULL)
	{
		FreeWaitEventSet(pageserver_conn_wes);
		pageserver_conn_wes = NULL;
	}
}

static void
pageserver_send(NeonRequest * request)
{
	StringInfoData req_buff;
	int n_reconnect_attempts = 0;

	/* If the connection was lost for some reason, reconnect */
	if (connected && PQstatus(pageserver_conn) == CONNECTION_BAD)
		pageserver_disconnect();


	req_buff = nm_pack_request(request);

	/*
	 * If pageserver is stopped, the connections from compute node are broken.
	 * The compute node doesn't notice that immediately, but it will cause the next request to fail, usually on the next query.
	 * That causes user-visible errors if pageserver is restarted, or the tenant is moved from one pageserver to another.
	 * See https://github.com/neondatabase/neon/issues/1138
	 * So try to reestablish connection in case of failure.
	 */
	while (true)
	{
		if (!connected)
		{
			if (!pageserver_connect(n_reconnect_attempts < MAX_RECONNECT_ATTEMPTS ? LOG : ERROR))
			{
				n_reconnect_attempts += 1;
				pg_usleep(RECONNECT_INTERVAL_USEC);
				continue;
			}
		}

		/*
		 * Send request.
		 *
		 * In principle, this could block if the output buffer is full, and we
		 * should use async mode and check for interrupts while waiting. In
		 * practice, our requests are small enough to always fit in the output and
		 * TCP buffer.
		 */
		if (PQputCopyData(pageserver_conn, req_buff.data, req_buff.len) <= 0)
		{
			char	   *msg = pchomp(PQerrorMessage(pageserver_conn));
			if (n_reconnect_attempts < MAX_RECONNECT_ATTEMPTS)
			{
				neon_log(LOG, "failed to send page request (try to reconnect): %s", msg);
				if (n_reconnect_attempts != 0) /* do not sleep before first reconnect attempt, assuming that pageserver is already restarted */
					pg_usleep(RECONNECT_INTERVAL_USEC);
				n_reconnect_attempts += 1;
				continue;
			}
			else
			{
				pageserver_disconnect();
				neon_log(ERROR, "failed to send page request: %s", msg);
			}
		}
		break;
	}

	pfree(req_buff.data);

	n_unflushed_requests++;

	if (flush_every_n_requests > 0 && n_unflushed_requests >= flush_every_n_requests)
		pageserver_flush();

	if (message_level_is_interesting(PageStoreTrace))
	{
		char	   *msg = nm_to_string((NeonMessage *) request);

		neon_log(PageStoreTrace, "sent request: %s", msg);
		pfree(msg);
	}
}

static NeonResponse *
pageserver_receive(void)
{
	StringInfoData resp_buff;
	NeonResponse *resp;

	if (!connected)
		return NULL;

	PG_TRY();
	{
		/* read response */
		int			rc;

		rc = call_PQgetCopyData(&resp_buff.data);
		if (rc >= 0)
		{
			resp_buff.len = rc;
			resp_buff.cursor = 0;
			resp = nm_unpack_response(&resp_buff);
			PQfreemem(resp_buff.data);

			if (message_level_is_interesting(PageStoreTrace))
			{
				char	   *msg = nm_to_string((NeonMessage *) resp);

				neon_log(PageStoreTrace, "got response: %s", msg);
				pfree(msg);
			}
		}
		else if (rc == -1)
		{
			pageserver_disconnect();
			resp = NULL;
		}
		else if (rc == -2)
			neon_log(ERROR, "could not read COPY data: %s", pchomp(PQerrorMessage(pageserver_conn)));
		else
			neon_log(ERROR, "unexpected PQgetCopyData return value: %d", rc);
	}
	PG_CATCH();
	{
		pageserver_disconnect();
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (NeonResponse *) resp;
}


static void
pageserver_flush(void)
{
	if (!connected)
	{
		neon_log(WARNING, "Tried to flush while disconnected");
	}
	else if (PQflush(pageserver_conn))
	{
		char	   *msg = pchomp(PQerrorMessage(pageserver_conn));

		pageserver_disconnect();
		neon_log(ERROR, "failed to flush page requests: %s", msg);
	}
	n_unflushed_requests = 0;
}

page_server_api api = {
	.send = pageserver_send,
	.flush = pageserver_flush,
	.receive = pageserver_receive
};

static bool
check_neon_id(char **newval, void **extra, GucSource source)
{
	uint8		id[16];

	return **newval == '\0' || HexDecodeString(id, *newval, 16);
}

/*
 * Module initialization function
 */
void
pg_init_libpagestore(void)
{
	DefineCustomStringVariable("neon.pageserver_connstring",
							   "connection string to the page server",
							   NULL,
							   &page_server_connstring,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

	DefineCustomStringVariable("neon.timeline_id",
							   "Neon timeline_id the server is running on",
							   NULL,
							   &neon_timeline,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomStringVariable("neon.tenant_id",
							   "Neon tenant_id the server is running on",
							   NULL,
							   &neon_tenant,
							   "",
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   check_neon_id, NULL, NULL);

	DefineCustomIntVariable("neon.max_cluster_size",
							"cluster size limit",
							NULL,
							&max_cluster_size,
							-1, -1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.flush_output_after",
							"Flush the output buffer after every N unflushed requests",
							NULL,
							&flush_every_n_requests,
							8, -1, INT_MAX,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, NULL, NULL);
	DefineCustomIntVariable("neon.readahead_buffer_size",
							"number of prefetches to buffer",
							"This buffer is used to hold and manage prefetched "
							"data; so it is important that this buffer is at "
							"least as large as the configured value of all "
							"tablespaces' effective_io_concurrency and "
							"maintenance_io_concurrency, and your sessions' "
							"values for these settings.",
							&readahead_buffer_size,
							128, 16, 1024,
							PGC_USERSET,
							0,	/* no flags required */
							NULL, (GucIntAssignHook) &readahead_buffer_resize, NULL);

	relsize_hash_init();

	if (page_server != NULL)
		neon_log(ERROR, "libpagestore already loaded");

	neon_log(PageStoreTrace, "libpagestore already loaded");
	page_server = &api;

	/* Retrieve the auth token to use when connecting to pageserver and safekeepers */
	neon_auth_token = getenv("NEON_AUTH_TOKEN");
	if (neon_auth_token)
		neon_log(LOG, "using storage auth token from NEON_AUTH_TOKEN environment variable");

	if (page_server_connstring && page_server_connstring[0])
	{
		neon_log(PageStoreTrace, "set neon_smgr hook");
		smgr_hook = smgr_neon;
		smgr_init_hook = smgr_init_neon;
		dbsize_hook = neon_dbsize;
		old_redo_read_buffer_filter = redo_read_buffer_filter;
		redo_read_buffer_filter = neon_redo_read_buffer_filter;
	}
	lfc_init();
}
