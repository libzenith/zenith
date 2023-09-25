/*-------------------------------------------------------------------------
 *
 * walproposer.c
 *
 * Proposer/leader part of the total order broadcast protocol between postgres
 * and WAL safekeepers.
 *
 * We have two ways of launching WalProposer:
 *
 *   1. As a background worker which will run physical WalSender with
 *      am_wal_proposer flag set to true. WalSender in turn would handle WAL
 *      reading part and call WalProposer when ready to scatter WAL.
 *
 *   2. As a standalone utility by running `postgres --sync-safekeepers`. That
 *      is needed to create LSN from which it is safe to start postgres. More
 *      specifically it addresses following problems:
 *
 *      a) Chicken-or-the-egg problem: compute postgres needs data directory
 *         with non-rel files that are downloaded from pageserver by calling
 *         basebackup@LSN. This LSN is not arbitrary, it must include all
 *         previously committed transactions and defined through consensus
 *         voting, which happens... in walproposer, a part of compute node.
 *
 *      b) Just warranting such LSN is not enough, we must also actually commit
 *         it and make sure there is a safekeeper who knows this LSN is
 *         committed so WAL before it can be streamed to pageserver -- otherwise
 *         basebackup will hang waiting for WAL. Advancing commit_lsn without
 *         playing consensus game is impossible, so speculative 'let's just poll
 *         safekeepers, learn start LSN of future epoch and run basebackup'
 *         won't work.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "access/xlogutils.h"
#include "access/xloginsert.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "storage/fd.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xlog.h"
#include "libpq/pqformat.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#if PG_VERSION_NUM >= 160000
#include "replication/walsender_private.h"
#endif
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "neon.h"
#include "walproposer.h"
#include "neon_utils.h"

static bool syncSafekeepers = false;

static int	n_safekeepers = 0;
static int	quorum = 0;
static Safekeeper safekeeper[MAX_SAFEKEEPERS];
static XLogRecPtr availableLsn; /* WAL has been generated up to this point */
static XLogRecPtr lastSentCommitLsn;	/* last commitLsn broadcast to*
										 * safekeepers */
static ProposerGreeting greetRequest;
static VoteRequest voteRequest; /* Vote request for safekeeper */
/*
 *  Minimal LSN which may be needed for recovery of some safekeeper,
 *  record-aligned (first record which might not yet received by someone).
 */
static XLogRecPtr truncateLsn;

/*
 * Term of the proposer. We want our term to be highest and unique,
 * so we collect terms from safekeepers quorum, choose max and +1.
 * After that our term is fixed and must not change. If we observe
 * that some safekeeper has higher term, it means that we have another
 * running compute, so we must stop immediately.
 */
static term_t propTerm;
static TermHistory propTermHistory; /* term history of the proposer */
static XLogRecPtr propEpochStartLsn;	/* epoch start lsn of the proposer */
static term_t donorEpoch;		/* Most advanced acceptor epoch */
static int	donor;				/* Most advanced acceptor */
static XLogRecPtr timelineStartLsn; /* timeline globally starts at this LSN */
static int	n_votes = 0;
static int	n_connected = 0;
static TimestampTz last_reconnect_attempt;

/* Prototypes for private functions */
static void WalProposerRegister(void);
static void WalProposerInit(XLogRecPtr flushRecPtr, uint64 systemId);
static void WalProposerStart(void);
static void WalProposerLoop(void);
static void HackyRemoveWalProposerEvent(Safekeeper *to_remove);
static void ShutdownConnection(Safekeeper *sk);
static void ResetConnection(Safekeeper *sk);
static long TimeToReconnect(TimestampTz now);
static void ReconnectSafekeepers(void);
static void AdvancePollState(Safekeeper *sk, uint32 events);
static void HandleConnectionEvent(Safekeeper *sk);
static void SendStartWALPush(Safekeeper *sk);
static void RecvStartWALPushResult(Safekeeper *sk);
static void SendProposerGreeting(Safekeeper *sk);
static void RecvAcceptorGreeting(Safekeeper *sk);
static void SendVoteRequest(Safekeeper *sk);
static void RecvVoteResponse(Safekeeper *sk);
static void HandleElectedProposer(void);
static term_t GetHighestTerm(TermHistory * th);
static term_t GetEpoch(Safekeeper *sk);
static void DetermineEpochStartLsn(void);
static void SendProposerElected(Safekeeper *sk);
static void StartStreaming(Safekeeper *sk);
static void SendMessageToNode(Safekeeper *sk);
static void BroadcastAppendRequest(void);
static void HandleActiveState(Safekeeper *sk, uint32 events);
static bool SendAppendRequests(Safekeeper *sk);
static bool RecvAppendResponses(Safekeeper *sk);
static XLogRecPtr CalculateMinFlushLsn(void);
static XLogRecPtr GetAcknowledgedByQuorumWALPosition(void);
static void HandleSafekeeperResponse(void);
static bool AsyncRead(Safekeeper *sk, char **buf, int *buf_size);
static bool AsyncReadMessage(Safekeeper *sk, AcceptorProposerMessage * anymsg);
static bool BlockingWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState success_state);
static bool AsyncWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState flush_state);
static bool AsyncFlush(Safekeeper *sk);

static int			CompareLsn(const void *a, const void *b);
static char	   *FormatSafekeeperState(SafekeeperState state);
static void		AssertEventsOkForState(uint32 events, Safekeeper *sk);
static uint32		SafekeeperStateDesiredEvents(SafekeeperState state);
static char	   *FormatEvents(uint32 events);

/*
 * Entry point for `postgres --sync-safekeepers`.
 */
PGDLLEXPORT void
WalProposerSync(int argc, char *argv[])
{
	syncSafekeepers = true;
	walprop_pg.init_standalone_sync_safekeepers();

	WalProposerInit(0, 0);
	WalProposerStart();
}

/*
 * WAL proposer bgworker entry point.
 */
PGDLLEXPORT void
WalProposerMain(Datum main_arg)
{
	uint64 systemId = walprop_pg.init_bgworker();

	WalProposerInit(walprop_pg.get_flush_rec_ptr(), systemId);

	last_reconnect_attempt = walprop_pg.get_current_timestamp();

	walprop_pg.init_walsender();

	WalProposerStart();
}

/*
 * Create new AppendRequest message and start sending it. This function is
 * called from walsender every time the new WAL is available.
 */
void
WalProposerBroadcast(XLogRecPtr startpos, XLogRecPtr endpos)
{
	Assert(startpos == availableLsn && endpos >= availableLsn);
	availableLsn = endpos;
	BroadcastAppendRequest();
}

/*
 * Advance the WAL proposer state machine, waiting each time for events to occur.
 * Will exit only when latch is set, i.e. new WAL should be pushed from walsender
 * to walproposer.
 */
void
WalProposerPoll(void)
{
	while (true)
	{
		Safekeeper *sk = NULL;
		int			rc = 0;
		uint32		events = 0;
		TimestampTz now = walprop_pg.get_current_timestamp();
		long		timeout = TimeToReconnect(now);

		rc = walprop_pg.wait_event_set(timeout, &sk, &events);

		/* Exit loop if latch is set (we got new WAL) */
		if ((rc == 1 && events & WL_LATCH_SET))
			break;
		
		/*
		 * If the event contains something that one of our safekeeper states
		 * was waiting for, we'll advance its state.
		 */
		if (rc == 1 && (events & WL_SOCKET_MASK))
		{
			Assert(sk != NULL);
			AdvancePollState(sk, events);
		}

		/*
		 * If the timeout expired, attempt to reconnect to any safekeepers
		 * that we dropped
		 */
		ReconnectSafekeepers();

		if (rc == 0) /* timeout expired */
		{
			/*
			 * Ensure flushrecptr is set to a recent value. This fixes a case
			 * where we've not been notified of new WAL records when we were
			 * planning on consuming them.
			 */
			if (!syncSafekeepers) {
				XLogRecPtr flushed = walprop_pg.get_flush_rec_ptr();

				if (flushed > availableLsn)
					break;
			}
		}

		now = walprop_pg.get_current_timestamp();
		if (rc == 0 || TimeToReconnect(now) <= 0)			/* timeout expired: poll state */
		{
			TimestampTz now;

			/*
			 * If no WAL was generated during timeout (and we have already
			 * collected the quorum), then send pool message
			 */
			if (availableLsn != InvalidXLogRecPtr)
			{
				BroadcastAppendRequest();
			}

			/*
			 * Abandon connection attempts which take too long.
			 */
			now = walprop_pg.get_current_timestamp();
			for (int i = 0; i < n_safekeepers; i++)
			{
				Safekeeper *sk = &safekeeper[i];

				if (TimestampDifferenceExceeds(sk->latestMsgReceivedAt, now,
											   wal_acceptor_connection_timeout))
				{
					elog(WARNING, "terminating connection to safekeeper '%s:%s' in '%s' state: no messages received during the last %dms or connection attempt took longer than that",
						 sk->host, sk->port, FormatSafekeeperState(sk->state), wal_acceptor_connection_timeout);
					ShutdownConnection(sk);
				}
			}
		}
	}
}

static void
WalProposerInit(XLogRecPtr flushRecPtr, uint64 systemId)
{
	char	   *host;
	char	   *sep;
	char	   *port;

	walprop_pg.load_libpqwalreceiver();

	for (host = wal_acceptors_list; host != NULL && *host != '\0'; host = sep)
	{
		port = strchr(host, ':');
		if (port == NULL)
		{
			elog(FATAL, "port is not specified");
		}
		*port++ = '\0';
		sep = strchr(port, ',');
		if (sep != NULL)
			*sep++ = '\0';
		if (n_safekeepers + 1 >= MAX_SAFEKEEPERS)
		{
			elog(FATAL, "Too many safekeepers");
		}
		safekeeper[n_safekeepers].host = host;
		safekeeper[n_safekeepers].port = port;
		safekeeper[n_safekeepers].state = SS_OFFLINE;
		safekeeper[n_safekeepers].conn = NULL;

		{
			Safekeeper *sk = &safekeeper[n_safekeepers];
			int written = 0;

			written = snprintf((char *) &sk->conninfo, MAXCONNINFO,
							   "host=%s port=%s dbname=replication options='-c timeline_id=%s tenant_id=%s'",
							   sk->host, sk->port, neon_timeline, neon_tenant);
			if (written > MAXCONNINFO || written < 0)
				elog(FATAL, "could not create connection string for safekeeper %s:%s", sk->host, sk->port);
		}

		initStringInfo(&safekeeper[n_safekeepers].outbuf);
		safekeeper[n_safekeepers].xlogreader = walprop_pg.wal_reader_allocate();
		if (safekeeper[n_safekeepers].xlogreader == NULL)
			elog(FATAL, "Failed to allocate xlog reader");
		safekeeper[n_safekeepers].flushWrite = false;
		safekeeper[n_safekeepers].startStreamingAt = InvalidXLogRecPtr;
		safekeeper[n_safekeepers].streamingAt = InvalidXLogRecPtr;
		n_safekeepers += 1;
	}
	if (n_safekeepers < 1)
	{
		elog(FATAL, "Safekeepers addresses are not specified");
	}
	quorum = n_safekeepers / 2 + 1;

	/* Fill the greeting package */
	greetRequest.tag = 'g';
	greetRequest.protocolVersion = SK_PROTOCOL_VERSION;
	greetRequest.pgVersion = PG_VERSION_NUM;
	walprop_pg.strong_random(&greetRequest.proposerId, sizeof(greetRequest.proposerId));
	greetRequest.systemId = systemId;
	if (!neon_timeline)
		elog(FATAL, "neon.timeline_id is not provided");
	if (*neon_timeline != '\0' &&
		!HexDecodeString(greetRequest.timeline_id, neon_timeline, 16))
		elog(FATAL, "Could not parse neon.timeline_id, %s", neon_timeline);
	if (!neon_tenant)
		elog(FATAL, "neon.tenant_id is not provided");
	if (*neon_tenant != '\0' &&
		!HexDecodeString(greetRequest.tenant_id, neon_tenant, 16))
		elog(FATAL, "Could not parse neon.tenant_id, %s", neon_tenant);

	greetRequest.timeline = walprop_pg.get_timeline_id();
	greetRequest.walSegSize = wal_segment_size;

	walprop_pg.init_event_set(n_safekeepers);
}

static void
WalProposerStart(void)
{

	/* Initiate connections to all safekeeper nodes */
	for (int i = 0; i < n_safekeepers; i++)
	{
		ResetConnection(&safekeeper[i]);
	}

	WalProposerLoop();
}

static void
WalProposerLoop(void)
{
	while (true)
		WalProposerPoll();
}

/*
 * Hack: provides a way to remove the event corresponding to an individual walproposer from the set.
 *
 * Note: Internally, this completely reconstructs the event set. It should be avoided if possible.
 */
static void
HackyRemoveWalProposerEvent(Safekeeper *to_remove)
{
	/* Remove the existing event set */
	walprop_pg.free_event_set();
	/* Re-initialize it without adding any safekeeper events */
	walprop_pg.init_event_set(n_safekeepers);

	/*
	 * loop through the existing safekeepers. If they aren't the one we're
	 * removing, and if they have a socket we can use, re-add the applicable
	 * events.
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
		uint32		desired_events = WL_NO_EVENTS;
		Safekeeper *sk = &safekeeper[i];

		sk->eventPos = -1;

		if (sk == to_remove)
			continue;

		/* If this safekeeper isn't offline, add an event for it! */
		if (sk->conn != NULL)
		{
			desired_events = SafekeeperStateDesiredEvents(sk->state);
			/* will set sk->eventPos */
			walprop_pg.add_safekeeper_event_set(sk, desired_events);
		}
	}
}

/* Shuts down and cleans up the connection for a safekeeper. Sets its state to SS_OFFLINE */
static void
ShutdownConnection(Safekeeper *sk)
{
	if (sk->conn)
		walprop_pg.conn_finish(sk->conn);
	sk->conn = NULL;
	sk->state = SS_OFFLINE;
	sk->flushWrite = false;
	sk->streamingAt = InvalidXLogRecPtr;

	if (sk->voteResponse.termHistory.entries)
		pfree(sk->voteResponse.termHistory.entries);
	sk->voteResponse.termHistory.entries = NULL;

	HackyRemoveWalProposerEvent(sk);
}

/*
 * This function is called to establish new connection or to reestablish
 * connection in case of connection failure.
 *
 * On success, sets the state to SS_CONNECTING_WRITE.
 */
static void
ResetConnection(Safekeeper *sk)
{
	if (sk->state != SS_OFFLINE)
	{
		ShutdownConnection(sk);
	}

	/*
	 * Try to establish new connection
	 */
	sk->conn = walprop_pg.conn_connect_start((char *) &sk->conninfo, neon_auth_token);

	/*
	 * "If the result is null, then libpq has been unable to allocate a new
	 * PGconn structure"
	 */
	if (!sk->conn)
		elog(FATAL, "failed to allocate new PGconn object");

	/*
	 * PQconnectStart won't actually start connecting until we run
	 * PQconnectPoll. Before we do that though, we need to check that it
	 * didn't immediately fail.
	 */
	if (walprop_pg.conn_status(sk->conn) == WP_CONNECTION_BAD)
	{
		/*---
		 * According to libpq docs:
		 *   "If the result is CONNECTION_BAD, the connection attempt has already failed,
		 *    typically because of invalid connection parameters."
		 * We should report this failure. Do not print the exact `conninfo` as it may
		 * contain e.g. password. The error message should already provide enough information.
		 *
		 * https://www.postgresql.org/docs/devel/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
		 */
		elog(WARNING, "Immediate failure to connect with node '%s:%s':\n\terror: %s",
			 sk->host, sk->port, walprop_pg.conn_error_message(sk->conn));

		/*
		 * Even though the connection failed, we still need to clean up the
		 * object
		 */
		walprop_pg.conn_finish(sk->conn);
		sk->conn = NULL;
		return;
	}

	/*
	 * The documentation for PQconnectStart states that we should call
	 * PQconnectPoll in a loop until it returns PGRES_POLLING_OK or
	 * PGRES_POLLING_FAILED. The other two possible returns indicate whether
	 * we should wait for reading or writing on the socket. For the first
	 * iteration of the loop, we're expected to wait until the socket becomes
	 * writable.
	 *
	 * The wording of the documentation is a little ambiguous; thankfully
	 * there's an example in the postgres source itself showing this behavior.
	 * (see libpqrcv_connect, defined in
	 * src/backend/replication/libpqwalreceiver/libpqwalreceiver.c)
	 */
	elog(LOG, "connecting with node %s:%s", sk->host, sk->port);

	sk->state = SS_CONNECTING_WRITE;
	sk->latestMsgReceivedAt = walprop_pg.get_current_timestamp();

	walprop_pg.add_safekeeper_event_set(sk, WL_SOCKET_WRITEABLE);
	return;
}

/*
 * How much milliseconds left till we should attempt reconnection to
 * safekeepers? Returns 0 if it is already high time, -1 if we never reconnect
 * (do we actually need this?).
 */
static long
TimeToReconnect(TimestampTz now)
{
	TimestampTz passed;
	TimestampTz till_reconnect;

	if (wal_acceptor_reconnect_timeout <= 0)
		return -1;

	passed = now - last_reconnect_attempt;
	till_reconnect = wal_acceptor_reconnect_timeout * 1000 - passed;
	if (till_reconnect <= 0)
		return 0;
	return (long) (till_reconnect / 1000);
}

/* If the timeout has expired, attempt to reconnect to all offline safekeepers */
static void
ReconnectSafekeepers(void)
{
	TimestampTz now = walprop_pg.get_current_timestamp();

	if (TimeToReconnect(now) == 0)
	{
		last_reconnect_attempt = now;
		for (int i = 0; i < n_safekeepers; i++)
		{
			if (safekeeper[i].state == SS_OFFLINE)
				ResetConnection(&safekeeper[i]);
		}
	}
}

/*
 * Performs the logic for advancing the state machine of the specified safekeeper,
 * given that a certain set of events has occurred.
 */
static void
AdvancePollState(Safekeeper *sk, uint32 events)
{
	/*
	 * Sanity check. We assume further down that the operations don't block
	 * because the socket is ready.
	 */
	AssertEventsOkForState(events, sk);

	/* Execute the code corresponding to the current state */
	switch (sk->state)
	{
			/*
			 * safekeepers are only taken out of SS_OFFLINE by calls to
			 * ResetConnection
			 */
		case SS_OFFLINE:
			elog(FATAL, "Unexpected safekeeper %s:%s state advancement: is offline",
				 sk->host, sk->port);
			break;				/* actually unreachable, but prevents
								 * -Wimplicit-fallthrough */

			/*
			 * Both connecting states run the same logic. The only difference
			 * is the events they're expecting
			 */
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			HandleConnectionEvent(sk);
			break;

			/*
			 * Waiting for a successful CopyBoth response.
			 */
		case SS_WAIT_EXEC_RESULT:
			RecvStartWALPushResult(sk);
			break;

			/*
			 * Finish handshake comms: receive information about the
			 * safekeeper.
			 */
		case SS_HANDSHAKE_RECV:
			RecvAcceptorGreeting(sk);
			break;

			/*
			 * Voting is an idle state - we don't expect any events to
			 * trigger. Refer to the execution of SS_HANDSHAKE_RECV to see how
			 * nodes are transferred from SS_VOTING to sending actual vote
			 * requests.
			 */
		case SS_VOTING:
			elog(WARNING, "EOF from node %s:%s in %s state", sk->host,
				 sk->port, FormatSafekeeperState(sk->state));
			ResetConnection(sk);
			return;

			/* Read the safekeeper response for our candidate */
		case SS_WAIT_VERDICT:
			RecvVoteResponse(sk);
			break;

			/* Flush proposer announcement message */
		case SS_SEND_ELECTED_FLUSH:

			/*
			 * AsyncFlush ensures we only move on to SS_ACTIVE once the flush
			 * completes. If we still have more to do, we'll wait until the
			 * next poll comes along.
			 */
			if (!AsyncFlush(sk))
				return;

			/* flush is done, event set and state will be updated later */
			StartStreaming(sk);
			break;

			/*
			 * Idle state for waiting votes from quorum.
			 */
		case SS_IDLE:
			elog(WARNING, "EOF from node %s:%s in %s state", sk->host,
				 sk->port, FormatSafekeeperState(sk->state));
			ResetConnection(sk);
			return;

			/*
			 * Active state is used for streaming WAL and receiving feedback.
			 */
		case SS_ACTIVE:
			HandleActiveState(sk, events);
			break;
	}
}

static void
HandleConnectionEvent(Safekeeper *sk)
{
	WalProposerConnectPollStatusType result = walprop_pg.conn_connect_poll(sk->conn);

	/* The new set of events we'll wait on, after updating */
	uint32		new_events = WL_NO_EVENTS;

	switch (result)
	{
		case WP_CONN_POLLING_OK:
			elog(LOG, "connected with node %s:%s", sk->host,
				 sk->port);
			sk->latestMsgReceivedAt = walprop_pg.get_current_timestamp();
			/*
			 * We have to pick some event to update event set. We'll
			 * eventually need the socket to be readable, so we go with that.
			 */
			new_events = WL_SOCKET_READABLE;
			break;

			/*
			 * If we need to poll to finish connecting, continue doing that
			 */
		case WP_CONN_POLLING_READING:
			sk->state = SS_CONNECTING_READ;
			new_events = WL_SOCKET_READABLE;
			break;
		case WP_CONN_POLLING_WRITING:
			sk->state = SS_CONNECTING_WRITE;
			new_events = WL_SOCKET_WRITEABLE;
			break;

		case WP_CONN_POLLING_FAILED:
			elog(WARNING, "failed to connect to node '%s:%s': %s",
				 sk->host, sk->port, walprop_pg.conn_error_message(sk->conn));

			/*
			 * If connecting failed, we don't want to restart the connection
			 * because that might run us into a loop. Instead, shut it down --
			 * it'll naturally restart at a slower interval on calls to
			 * ReconnectSafekeepers.
			 */
			ShutdownConnection(sk);
			return;
	}

	/*
	 * Because PQconnectPoll can change the socket, we have to un-register the
	 * old event and re-register an event on the new socket.
	 */
	HackyRemoveWalProposerEvent(sk);
	walprop_pg.add_safekeeper_event_set(sk, new_events);

	/* If we successfully connected, send START_WAL_PUSH query */
	if (result == WP_CONN_POLLING_OK)
		SendStartWALPush(sk);
}

/*
 * Send "START_WAL_PUSH" message as an empty query to the safekeeper. Performs
 * a blocking send, then immediately moves to SS_WAIT_EXEC_RESULT. If something
 * goes wrong, change state to SS_OFFLINE and shutdown the connection.
 */
static void
SendStartWALPush(Safekeeper *sk)
{
	if (!walprop_pg.conn_send_query(sk->conn, "START_WAL_PUSH"))
	{
		elog(WARNING, "Failed to send 'START_WAL_PUSH' query to safekeeper %s:%s: %s",
			 sk->host, sk->port, walprop_pg.conn_error_message(sk->conn));
		ShutdownConnection(sk);
		return;
	}
	sk->state = SS_WAIT_EXEC_RESULT;
	walprop_pg.update_event_set(sk, WL_SOCKET_READABLE);
}

static void
RecvStartWALPushResult(Safekeeper *sk)
{
	switch (walprop_pg.conn_get_query_result(sk->conn))
	{
			/*
			 * Successful result, move on to starting the handshake
			 */
		case WP_EXEC_SUCCESS_COPYBOTH:

			SendProposerGreeting(sk);
			break;

			/*
			 * Needs repeated calls to finish. Wait until the socket is
			 * readable
			 */
		case WP_EXEC_NEEDS_INPUT:

			/*
			 * SS_WAIT_EXEC_RESULT is always reached through an event, so we
			 * don't need to update the event set
			 */
			break;

		case WP_EXEC_FAILED:
			elog(WARNING, "Failed to send query to safekeeper %s:%s: %s",
				 sk->host, sk->port, walprop_pg.conn_error_message(sk->conn));
			ShutdownConnection(sk);
			return;

			/*
			 * Unexpected result -- funamdentally an error, but we want to
			 * produce a custom message, rather than a generic "something went
			 * wrong"
			 */
		case WP_EXEC_UNEXPECTED_SUCCESS:
			elog(WARNING, "Received bad response from safekeeper %s:%s query execution",
				 sk->host, sk->port);
			ShutdownConnection(sk);
			return;
	}
}

/*
 * Start handshake: first of all send information about the
 * safekeeper. After sending, we wait on SS_HANDSHAKE_RECV for
 * a response to finish the handshake.
 */
static void
SendProposerGreeting(Safekeeper *sk)
{
	/*
	 * On failure, logging & resetting the connection is handled. We just need
	 * to handle the control flow.
	 */
	BlockingWrite(sk, &greetRequest, sizeof(greetRequest), SS_HANDSHAKE_RECV);
}

static void
RecvAcceptorGreeting(Safekeeper *sk)
{
	/*
	 * If our reading doesn't immediately succeed, any necessary error
	 * handling or state setting is taken care of. We can leave any other work
	 * until later.
	 */
	sk->greetResponse.apm.tag = 'g';
	if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->greetResponse))
		return;

	elog(LOG, "received AcceptorGreeting from safekeeper %s:%s", sk->host, sk->port);

	/* Protocol is all good, move to voting. */
	sk->state = SS_VOTING;

	/* 
	 * Note: it would be better to track the counter on per safekeeper basis,
	 * but at worst walproposer would restart with 'term rejected', so leave as
	 * is for now.
	 */
	++n_connected;
	if (n_connected <= quorum)
	{
		/* We're still collecting terms from the majority. */
		propTerm = Max(sk->greetResponse.term, propTerm);

		/* Quorum is acquried, prepare the vote request. */
		if (n_connected == quorum)
		{
			propTerm++;
			elog(LOG, "proposer connected to quorum (%d) safekeepers, propTerm=" INT64_FORMAT, quorum, propTerm);

			voteRequest = (VoteRequest)
			{
				.tag = 'v',
					.term = propTerm
			};
			memcpy(voteRequest.proposerId.data, greetRequest.proposerId.data, UUID_LEN);
		}
	}
	else if (sk->greetResponse.term > propTerm)
	{
		/* Another compute with higher term is running. */
		elog(FATAL, "WAL acceptor %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
			 sk->host, sk->port,
			 sk->greetResponse.term, propTerm);
	}

	/*
	 * Check if we have quorum. If there aren't enough safekeepers, wait and
	 * do nothing. We'll eventually get a task when the election starts.
	 *
	 * If we do have quorum, we can start an election.
	 */
	if (n_connected < quorum)
	{
		/*
		 * SS_VOTING is an idle state; read-ready indicates the connection
		 * closed.
		 */
		walprop_pg.update_event_set(sk, WL_SOCKET_READABLE);
	}
	else
	{
		/*
		 * Now send voting request to the cohort and wait responses
		 */
		for (int j = 0; j < n_safekeepers; j++)
		{
			/*
			 * Remember: SS_VOTING indicates that the safekeeper is
			 * participating in voting, but hasn't sent anything yet.
			 */
			if (safekeeper[j].state == SS_VOTING)
				SendVoteRequest(&safekeeper[j]);
		}
	}
}

static void
SendVoteRequest(Safekeeper *sk)
{
	/* We have quorum for voting, send our vote request */
	elog(LOG, "requesting vote from %s:%s for term " UINT64_FORMAT, sk->host, sk->port, voteRequest.term);
	/* On failure, logging & resetting is handled */
	if (!BlockingWrite(sk, &voteRequest, sizeof(voteRequest), SS_WAIT_VERDICT))
		return;

	/* If successful, wait for read-ready with SS_WAIT_VERDICT */
}

static void
RecvVoteResponse(Safekeeper *sk)
{
	sk->voteResponse.apm.tag = 'v';
	if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->voteResponse))
		return;

	elog(LOG,
		 "got VoteResponse from acceptor %s:%s, voteGiven=" UINT64_FORMAT ", epoch=" UINT64_FORMAT ", flushLsn=%X/%X, truncateLsn=%X/%X, timelineStartLsn=%X/%X",
		 sk->host, sk->port, sk->voteResponse.voteGiven, GetHighestTerm(&sk->voteResponse.termHistory),
		 LSN_FORMAT_ARGS(sk->voteResponse.flushLsn),
		 LSN_FORMAT_ARGS(sk->voteResponse.truncateLsn),
		 LSN_FORMAT_ARGS(sk->voteResponse.timelineStartLsn));

	/*
	 * In case of acceptor rejecting our vote, bail out, but only if either it
	 * already lives in strictly higher term (concurrent compute spotted) or
	 * we are not elected yet and thus need the vote.
	 */
	if ((!sk->voteResponse.voteGiven) &&
		(sk->voteResponse.term > propTerm || n_votes < quorum))
	{
		elog(FATAL, "WAL acceptor %s:%s with term " INT64_FORMAT " rejects our connection request with term " INT64_FORMAT "",
			 sk->host, sk->port,
			 sk->voteResponse.term, propTerm);
	}
	Assert(sk->voteResponse.term == propTerm);

	/* Handshake completed, do we have quorum? */
	n_votes++;
	if (n_votes < quorum)
	{
		sk->state = SS_IDLE;	/* can't do much yet, no quorum */
	}
	else if (n_votes > quorum)
	{
		/* recovery already performed, just start streaming */
		SendProposerElected(sk);
	}
	else
	{
		sk->state = SS_IDLE;
		/* Idle state waits for read-ready events */
		walprop_pg.update_event_set(sk, WL_SOCKET_READABLE);

		HandleElectedProposer();
	}
}

/*
 * Called once a majority of acceptors have voted for us and current proposer
 * has been elected.
 *
 * Sends ProposerElected message to all acceptors in SS_IDLE state and starts
 * replication from walsender.
 */
static void
HandleElectedProposer(void)
{
	DetermineEpochStartLsn();

	/*
	 * Check if not all safekeepers are up-to-date, we need to download WAL
	 * needed to synchronize them
	 */
	if (truncateLsn < propEpochStartLsn)
	{
		elog(LOG,
			 "start recovery because truncateLsn=%X/%X is not "
			 "equal to epochStartLsn=%X/%X",
			 LSN_FORMAT_ARGS(truncateLsn),
			 LSN_FORMAT_ARGS(propEpochStartLsn));
		/* Perform recovery */
		if (!walprop_pg.recovery_download(&safekeeper[donor], greetRequest.timeline, truncateLsn, propEpochStartLsn))
			elog(FATAL, "Failed to recover state");
	}
	else if (syncSafekeepers)
	{
		/* Sync is not needed: just exit */
		walprop_pg.finish_sync_safekeepers(propEpochStartLsn);
		/* unreachable */
	}

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE)
			SendProposerElected(&safekeeper[i]);
	}

	/*
	 * The proposer has been elected, and there will be no quorum waiting
	 * after this point. There will be no safekeeper with state SS_IDLE also,
	 * because that state is used only for quorum waiting.
	 */

	if (syncSafekeepers)
	{
		/*
		 * Send empty message to enforce receiving feedback even from nodes
		 * who are fully recovered; this is required to learn they switched
		 * epoch which finishes sync-safeekepers who doesn't generate any real
		 * new records. Will go away once we switch to async acks.
		 */
		BroadcastAppendRequest();

		/* keep polling until all safekeepers are synced */
		return;
	}

	walprop_pg.start_streaming(propEpochStartLsn, greetRequest.timeline);
	/* Should not return here */
}

/* latest term in TermHistory, or 0 is there is no entries */
static term_t
GetHighestTerm(TermHistory * th)
{
	return th->n_entries > 0 ? th->entries[th->n_entries - 1].term : 0;
}

/* safekeeper's epoch is the term of the highest entry in the log */
static term_t
GetEpoch(Safekeeper *sk)
{
	return GetHighestTerm(&sk->voteResponse.termHistory);
}

/* If LSN points to the page header, skip it */
static XLogRecPtr
SkipXLogPageHeader(XLogRecPtr lsn)
{
	if (XLogSegmentOffset(lsn, wal_segment_size) == 0)
	{
		lsn += SizeOfXLogLongPHD;
	}
	else if (lsn % XLOG_BLCKSZ == 0)
	{
		lsn += SizeOfXLogShortPHD;
	}
	return lsn;
}

/*
 * Called after majority of acceptors gave votes, it calculates the most
 * advanced safekeeper (who will be the donor) and epochStartLsn -- LSN since
 * which we'll write WAL in our term.
 *
 * Sets truncateLsn along the way (though it is not of much use at this point --
 * only for skipping recovery).
 */
static void
DetermineEpochStartLsn(void)
{
	TermHistory *dth;

	propEpochStartLsn = InvalidXLogRecPtr;
	donorEpoch = 0;
	truncateLsn = InvalidXLogRecPtr;
	timelineStartLsn = InvalidXLogRecPtr;

	for (int i = 0; i < n_safekeepers; i++)
	{
		if (safekeeper[i].state == SS_IDLE)
		{
			if (GetEpoch(&safekeeper[i]) > donorEpoch ||
				(GetEpoch(&safekeeper[i]) == donorEpoch &&
				 safekeeper[i].voteResponse.flushLsn > propEpochStartLsn))
			{
				donorEpoch = GetEpoch(&safekeeper[i]);
				propEpochStartLsn = safekeeper[i].voteResponse.flushLsn;
				donor = i;
			}
			truncateLsn = Max(safekeeper[i].voteResponse.truncateLsn, truncateLsn);

			if (safekeeper[i].voteResponse.timelineStartLsn != InvalidXLogRecPtr)
			{
				/* timelineStartLsn should be the same everywhere or unknown */
				if (timelineStartLsn != InvalidXLogRecPtr &&
					timelineStartLsn != safekeeper[i].voteResponse.timelineStartLsn)
				{
					elog(WARNING,
						 "inconsistent timelineStartLsn: current %X/%X, received %X/%X",
						 LSN_FORMAT_ARGS(timelineStartLsn),
						 LSN_FORMAT_ARGS(safekeeper[i].voteResponse.timelineStartLsn));
				}
				timelineStartLsn = safekeeper[i].voteResponse.timelineStartLsn;
			}
		}
	}

	/*
	 * If propEpochStartLsn is 0 everywhere, we are bootstrapping -- nothing
	 * was committed yet. Start streaming then from the basebackup LSN.
	 */
	if (propEpochStartLsn == InvalidXLogRecPtr && !syncSafekeepers)
	{
		propEpochStartLsn = truncateLsn = walprop_pg.get_redo_start_lsn();
		if (timelineStartLsn == InvalidXLogRecPtr)
		{
			timelineStartLsn = walprop_pg.get_redo_start_lsn();
		}
		elog(LOG, "bumped epochStartLsn to the first record %X/%X", LSN_FORMAT_ARGS(propEpochStartLsn));
	}

	/*
	 * If propEpochStartLsn is not 0, at least one msg with WAL was sent to
	 * some connected safekeeper; it must have carried truncateLsn pointing to
	 * the first record.
	 */
	Assert((truncateLsn != InvalidXLogRecPtr) ||
		   (syncSafekeepers && truncateLsn == propEpochStartLsn));

	/*
	 * We will be generating WAL since propEpochStartLsn, so we should set
	 * availableLsn to mark this LSN as the latest available position.
	 */
	availableLsn = propEpochStartLsn;

	/*
	 * Proposer's term history is the donor's + its own entry.
	 */
	dth = &safekeeper[donor].voteResponse.termHistory;
	propTermHistory.n_entries = dth->n_entries + 1;
	propTermHistory.entries = palloc(sizeof(TermSwitchEntry) * propTermHistory.n_entries);
	memcpy(propTermHistory.entries, dth->entries, sizeof(TermSwitchEntry) * dth->n_entries);
	propTermHistory.entries[propTermHistory.n_entries - 1].term = propTerm;
	propTermHistory.entries[propTermHistory.n_entries - 1].lsn = propEpochStartLsn;

	elog(LOG, "got votes from majority (%d) of nodes, term " UINT64_FORMAT ", epochStartLsn %X/%X, donor %s:%s, truncate_lsn %X/%X",
		 quorum,
		 propTerm,
		 LSN_FORMAT_ARGS(propEpochStartLsn),
		 safekeeper[donor].host, safekeeper[donor].port,
		 LSN_FORMAT_ARGS(truncateLsn));

	/*
	 * Ensure the basebackup we are running (at RedoStartLsn) matches LSN
	 * since which we are going to write according to the consensus. If not,
	 * we must bail out, as clog and other non rel data is inconsistent.
	 */
	if (!syncSafekeepers)
	{
		WalproposerShmemState * walprop_shared = walprop_pg.get_shmem_state();

		/*
		 * Basebackup LSN always points to the beginning of the record (not
		 * the page), as StartupXLOG most probably wants it this way.
		 * Safekeepers don't skip header as they need continious stream of
		 * data, so correct LSN for comparison.
		 */
		if (SkipXLogPageHeader(propEpochStartLsn) != walprop_pg.get_redo_start_lsn())
		{
			/*
			 * However, allow to proceed if previously elected leader was me;
			 * plain restart of walproposer not intervened by concurrent
			 * compute (who could generate WAL) is ok.
			 */
			if (!((dth->n_entries >= 1) && (dth->entries[dth->n_entries - 1].term ==
											walprop_shared->mineLastElectedTerm)))
			{
				elog(PANIC,
					 "collected propEpochStartLsn %X/%X, but basebackup LSN %X/%X",
					 LSN_FORMAT_ARGS(propEpochStartLsn),
					 LSN_FORMAT_ARGS(walprop_pg.get_redo_start_lsn()));
			}
		}
		walprop_shared->mineLastElectedTerm = propTerm;
	}
}

/*
 * Determine for sk the starting streaming point and send it message
 * 1) Announcing we are elected proposer (which immediately advances epoch if
 *    safekeeper is synced, being important for sync-safekeepers)
 * 2) Communicating starting streaming point -- safekeeper must truncate its WAL
 *    beyond it -- and history of term switching.
 *
 * Sets sk->startStreamingAt.
 */
static void
SendProposerElected(Safekeeper *sk)
{
	ProposerElected msg;
	TermHistory *th;
	term_t		lastCommonTerm;
	int			i;

	/*
	 * Determine start LSN by comparing safekeeper's log term switch history
	 * and proposer's, searching for the divergence point.
	 *
	 * Note: there is a vanishingly small chance of no common point even if
	 * there is some WAL on safekeeper, if immediately after bootstrap compute
	 * wrote some WAL on single sk and died; we stream since the beginning
	 * then.
	 */
	th = &sk->voteResponse.termHistory;

	/* We must start somewhere. */
	Assert(propTermHistory.n_entries >= 1);

	for (i = 0; i < Min(propTermHistory.n_entries, th->n_entries); i++)
	{
		if (propTermHistory.entries[i].term != th->entries[i].term)
			break;
		/* term must begin everywhere at the same point */
		Assert(propTermHistory.entries[i].lsn == th->entries[i].lsn);
	}
	i--;						/* step back to the last common term */
	if (i < 0)
	{
		/* safekeeper is empty or no common point, start from the beginning */
		sk->startStreamingAt = propTermHistory.entries[0].lsn;

		if (sk->startStreamingAt < truncateLsn)
		{
			/*
			 * There's a gap between the WAL starting point and a truncateLsn,
			 * which can't appear in a normal working cluster. That gap means
			 * that all safekeepers reported that they have persisted WAL up
			 * to the truncateLsn before, but now current safekeeper tells
			 * otherwise.
			 *
			 * Also we have a special condition here, which is empty
			 * safekeeper with no history. In combination with a gap, that can
			 * happen when we introduce a new safekeeper to the cluster. This
			 * is a rare case, which is triggered manually for now, and should
			 * be treated with care.
			 */

			/*
			 * truncateLsn will not change without ack from current
			 * safekeeper, and it's aligned to the WAL record, so we can
			 * safely start streaming from this point.
			 */
			sk->startStreamingAt = truncateLsn;

			elog(WARNING, "empty safekeeper joined cluster as %s:%s, historyStart=%X/%X, sk->startStreamingAt=%X/%X",
				 sk->host, sk->port, LSN_FORMAT_ARGS(propTermHistory.entries[0].lsn),
				 LSN_FORMAT_ARGS(sk->startStreamingAt));
		}
	}
	else
	{
		/*
		 * End of (common) term is the start of the next except it is the last
		 * one; there it is flush_lsn in case of safekeeper or, in case of
		 * proposer, LSN it is currently writing, but then we just pick
		 * safekeeper pos as it obviously can't be higher.
		 */
		if (propTermHistory.entries[i].term == propTerm)
		{
			sk->startStreamingAt = sk->voteResponse.flushLsn;
		}
		else
		{
			XLogRecPtr	propEndLsn = propTermHistory.entries[i + 1].lsn;
			XLogRecPtr	skEndLsn = (i + 1 < th->n_entries ? th->entries[i + 1].lsn : sk->voteResponse.flushLsn);

			sk->startStreamingAt = Min(propEndLsn, skEndLsn);
		}
	}

	Assert(sk->startStreamingAt >= truncateLsn && sk->startStreamingAt <= availableLsn);

	msg.tag = 'e';
	msg.term = propTerm;
	msg.startStreamingAt = sk->startStreamingAt;
	msg.termHistory = &propTermHistory;
	msg.timelineStartLsn = timelineStartLsn;

	lastCommonTerm = i >= 0 ? propTermHistory.entries[i].term : 0;
	elog(LOG,
		 "sending elected msg to node " UINT64_FORMAT " term=" UINT64_FORMAT ", startStreamingAt=%X/%X (lastCommonTerm=" UINT64_FORMAT "), termHistory.n_entries=%u to %s:%s, timelineStartLsn=%X/%X",
		 sk->greetResponse.nodeId, msg.term, LSN_FORMAT_ARGS(msg.startStreamingAt), lastCommonTerm, msg.termHistory->n_entries, sk->host, sk->port, LSN_FORMAT_ARGS(msg.timelineStartLsn));

	resetStringInfo(&sk->outbuf);
	pq_sendint64_le(&sk->outbuf, msg.tag);
	pq_sendint64_le(&sk->outbuf, msg.term);
	pq_sendint64_le(&sk->outbuf, msg.startStreamingAt);
	pq_sendint32_le(&sk->outbuf, msg.termHistory->n_entries);
	for (int i = 0; i < msg.termHistory->n_entries; i++)
	{
		pq_sendint64_le(&sk->outbuf, msg.termHistory->entries[i].term);
		pq_sendint64_le(&sk->outbuf, msg.termHistory->entries[i].lsn);
	}
	pq_sendint64_le(&sk->outbuf, msg.timelineStartLsn);

	if (!AsyncWrite(sk, sk->outbuf.data, sk->outbuf.len, SS_SEND_ELECTED_FLUSH))
		return;

	StartStreaming(sk);
}

/*
 * Start streaming to safekeeper sk, always updates state to SS_ACTIVE and sets
 * correct event set.
 */
static void
StartStreaming(Safekeeper *sk)
{
	/*
	 * This is the only entrypoint to state SS_ACTIVE. It's executed exactly
	 * once for a connection.
	 */
	sk->state = SS_ACTIVE;
	sk->streamingAt = sk->startStreamingAt;

	/* event set will be updated inside SendMessageToNode */
	SendMessageToNode(sk);
}

/*
 * Try to send message to the particular node. Always updates event set. Will
 * send at least one message, if socket is ready.
 *
 * Can be used only for safekeepers in SS_ACTIVE state. State can be changed
 * in case of errors.
 */
static void
SendMessageToNode(Safekeeper *sk)
{
	Assert(sk->state == SS_ACTIVE);

	/*
	 * Note: we always send everything to the safekeeper until WOULDBLOCK or
	 * nothing left to send
	 */
	HandleActiveState(sk, WL_SOCKET_WRITEABLE);
}

/*
 * Broadcast new message to all caught-up safekeepers
 */
static void
BroadcastAppendRequest()
{
	for (int i = 0; i < n_safekeepers; i++)
		if (safekeeper[i].state == SS_ACTIVE)
			SendMessageToNode(&safekeeper[i]);
}

static void
PrepareAppendRequest(AppendRequestHeader * req, XLogRecPtr beginLsn, XLogRecPtr endLsn)
{
	Assert(endLsn >= beginLsn);
	req->tag = 'a';
	req->term = propTerm;
	req->epochStartLsn = propEpochStartLsn;
	req->beginLsn = beginLsn;
	req->endLsn = endLsn;
	req->commitLsn = GetAcknowledgedByQuorumWALPosition();
	req->truncateLsn = truncateLsn;
	req->proposerId = greetRequest.proposerId;
}

/*
 * Process all events happened in SS_ACTIVE state, update event set after that.
 */
static void
HandleActiveState(Safekeeper *sk, uint32 events)
{
	uint32		newEvents = WL_SOCKET_READABLE;

	if (events & WL_SOCKET_WRITEABLE)
		if (!SendAppendRequests(sk))
			return;

	if (events & WL_SOCKET_READABLE)
		if (!RecvAppendResponses(sk))
			return;

	/*
	 * We should wait for WL_SOCKET_WRITEABLE event if we have unflushed data
	 * in the buffer.
	 *
	 * LSN comparison checks if we have pending unsent messages. This check
	 * isn't necessary now, because we always send append messages immediately
	 * after arrival. But it's good to have it here in case we change this
	 * behavior in the future.
	 */
	if (sk->streamingAt != availableLsn || sk->flushWrite)
		newEvents |= WL_SOCKET_WRITEABLE;

	walprop_pg.update_event_set(sk, newEvents);
}

/*
 * Send WAL messages starting from sk->streamingAt until the end or non-writable
 * socket, whichever comes first. Caller should take care of updating event set.
 * Even if no unsent WAL is available, at least one empty message will be sent
 * as a heartbeat, if socket is ready.
 *
 * Can change state if Async* functions encounter errors and reset connection.
 * Returns false in this case, true otherwise.
 */
static bool
SendAppendRequests(Safekeeper *sk)
{
	XLogRecPtr	endLsn;
	AppendRequestHeader *req;
	PGAsyncWriteResult writeResult;
	bool		sentAnything = false;

	if (sk->flushWrite)
	{
		if (!AsyncFlush(sk))

			/*
			 * AsyncFlush failed, that could happen if the socket is closed or
			 * we have nothing to write and should wait for writeable socket.
			 */
			return sk->state == SS_ACTIVE;

		/* Event set will be updated in the end of HandleActiveState */
		sk->flushWrite = false;
	}

	while (sk->streamingAt != availableLsn || !sentAnything)
	{
		sentAnything = true;

		endLsn = sk->streamingAt;
		endLsn += MAX_SEND_SIZE;

		/* if we went beyond available WAL, back off */
		if (endLsn > availableLsn)
		{
			endLsn = availableLsn;
		}

		req = &sk->appendRequest;
		PrepareAppendRequest(&sk->appendRequest, sk->streamingAt, endLsn);

		ereport(DEBUG2,
				(errmsg("sending message len %ld beginLsn=%X/%X endLsn=%X/%X commitLsn=%X/%X truncateLsn=%X/%X to %s:%s",
						req->endLsn - req->beginLsn,
						LSN_FORMAT_ARGS(req->beginLsn),
						LSN_FORMAT_ARGS(req->endLsn),
						LSN_FORMAT_ARGS(req->commitLsn),
						LSN_FORMAT_ARGS(truncateLsn), sk->host, sk->port)));

		resetStringInfo(&sk->outbuf);

		/* write AppendRequest header */
		appendBinaryStringInfo(&sk->outbuf, (char *) req, sizeof(AppendRequestHeader));

		/* write the WAL itself */
		enlargeStringInfo(&sk->outbuf, req->endLsn - req->beginLsn);
		/* wal_read will raise error on failure */
		walprop_pg.wal_read(sk->xlogreader,
					 &sk->outbuf.data[sk->outbuf.len],
					 req->beginLsn,
					 req->endLsn - req->beginLsn);
		sk->outbuf.len += req->endLsn - req->beginLsn;

		writeResult = walprop_pg.conn_async_write(sk->conn, sk->outbuf.data, sk->outbuf.len);

		/* Mark current message as sent, whatever the result is */
		sk->streamingAt = endLsn;

		switch (writeResult)
		{
			case PG_ASYNC_WRITE_SUCCESS:
				/* Continue writing the next message */
				break;

			case PG_ASYNC_WRITE_TRY_FLUSH:

				/*
				 * * We still need to call PQflush some more to finish the
				 * job. Caller function will handle this by setting right
				 * event* set.
				 */
				sk->flushWrite = true;
				return true;

			case PG_ASYNC_WRITE_FAIL:
				elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
					 sk->host, sk->port, FormatSafekeeperState(sk->state),
					 walprop_pg.conn_error_message(sk->conn));
				ShutdownConnection(sk);
				return false;
			default:
				Assert(false);
				return false;
		}
	}

	return true;
}

/*
 * Receive and process all available feedback.
 *
 * Can change state if Async* functions encounter errors and reset connection.
 * Returns false in this case, true otherwise.
 *
 * NB: This function can call SendMessageToNode and produce new messages.
 */
static bool
RecvAppendResponses(Safekeeper *sk)
{
	XLogRecPtr	minQuorumLsn;
	bool		readAnything = false;

	while (true)
	{
		/*
		 * If our reading doesn't immediately succeed, any necessary error
		 * handling or state setting is taken care of. We can leave any other
		 * work until later.
		 */
		sk->appendResponse.apm.tag = 'a';
		if (!AsyncReadMessage(sk, (AcceptorProposerMessage *) & sk->appendResponse))
			break;

		ereport(DEBUG2,
				(errmsg("received message term=" INT64_FORMAT " flushLsn=%X/%X commitLsn=%X/%X from %s:%s",
						sk->appendResponse.term,
						LSN_FORMAT_ARGS(sk->appendResponse.flushLsn),
						LSN_FORMAT_ARGS(sk->appendResponse.commitLsn),
						sk->host, sk->port)));

		if (sk->appendResponse.term > propTerm)
		{
			/* Another compute with higher term is running. */
			elog(PANIC, "WAL acceptor %s:%s with term " INT64_FORMAT " rejected our request, our term " INT64_FORMAT "",
				 sk->host, sk->port,
				 sk->appendResponse.term, propTerm);
		}

		readAnything = true;
	}

	if (!readAnything)
		return sk->state == SS_ACTIVE;

	HandleSafekeeperResponse();

	/*
	 * Also send the new commit lsn to all the safekeepers.
	 */
	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	if (minQuorumLsn > lastSentCommitLsn)
	{
		BroadcastAppendRequest();
		lastSentCommitLsn = minQuorumLsn;
	}

	return sk->state == SS_ACTIVE;
}

/* Parse a PageserverFeedback message, or the PageserverFeedback part of an AppendResponse */
void
ParsePageserverFeedbackMessage(StringInfo reply_message, PageserverFeedback * rf)
{
	uint8		nkeys;
	int			i;
	int32		len;

	/* get number of custom keys */
	nkeys = pq_getmsgbyte(reply_message);

	for (i = 0; i < nkeys; i++)
	{
		const char *key = pq_getmsgstring(reply_message);

		if (strcmp(key, "current_timeline_size") == 0)
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->currentClusterSize = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParsePageserverFeedbackMessage: current_timeline_size %lu",
				 rf->currentClusterSize);
		}
		else if ((strcmp(key, "ps_writelsn") == 0) || (strcmp(key, "last_received_lsn") == 0))
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->last_received_lsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParsePageserverFeedbackMessage: last_received_lsn %X/%X",
				 LSN_FORMAT_ARGS(rf->last_received_lsn));
		}
		else if ((strcmp(key, "ps_flushlsn") == 0) || (strcmp(key, "disk_consistent_lsn") == 0))
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->disk_consistent_lsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParsePageserverFeedbackMessage: disk_consistent_lsn %X/%X",
				 LSN_FORMAT_ARGS(rf->disk_consistent_lsn));
		}
		else if ((strcmp(key, "ps_applylsn") == 0) || (strcmp(key, "remote_consistent_lsn") == 0))
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->remote_consistent_lsn = pq_getmsgint64(reply_message);
			elog(DEBUG2, "ParsePageserverFeedbackMessage: remote_consistent_lsn %X/%X",
				 LSN_FORMAT_ARGS(rf->remote_consistent_lsn));
		}
		else if ((strcmp(key, "ps_replytime") == 0) || (strcmp(key, "replytime") == 0))
		{
			pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */
			rf->replytime = pq_getmsgint64(reply_message);
			{
				char	   *replyTimeStr;

				/* Copy because timestamptz_to_str returns a static buffer */
				replyTimeStr = pstrdup(timestamptz_to_str(rf->replytime));
				elog(DEBUG2, "ParsePageserverFeedbackMessage: replytime %lu reply_time: %s",
					 rf->replytime, replyTimeStr);

				pfree(replyTimeStr);
			}
		}
		else
		{
			len = pq_getmsgint(reply_message, sizeof(int32));
			/* read value length */

			/*
			 * Skip unknown keys to support backward compatibile protocol
			 * changes
			 */
			elog(LOG, "ParsePageserverFeedbackMessage: unknown key: %s len %d", key, len);
			pq_getmsgbytes(reply_message, len);
		};
	}
}

/*
 * Get minimum of flushed LSNs of all safekeepers, which is the LSN of the
 * last WAL record that can be safely discarded.
 */
static XLogRecPtr
CalculateMinFlushLsn(void)
{
	XLogRecPtr	lsn = n_safekeepers > 0
	? safekeeper[0].appendResponse.flushLsn
	: InvalidXLogRecPtr;

	for (int i = 1; i < n_safekeepers; i++)
	{
		lsn = Min(lsn, safekeeper[i].appendResponse.flushLsn);
	}
	return lsn;
}

/*
 * Calculate WAL position acknowledged by quorum
 */
static XLogRecPtr
GetAcknowledgedByQuorumWALPosition(void)
{
	XLogRecPtr	responses[MAX_SAFEKEEPERS];

	/*
	 * Sort acknowledged LSNs
	 */
	for (int i = 0; i < n_safekeepers; i++)
	{
		/*
		 * Like in Raft, we aren't allowed to commit entries from previous
		 * terms, so ignore reported LSN until it gets to epochStartLsn.
		 */
		responses[i] = safekeeper[i].appendResponse.flushLsn >= propEpochStartLsn ? safekeeper[i].appendResponse.flushLsn : 0;
	}
	qsort(responses, n_safekeepers, sizeof(XLogRecPtr), CompareLsn);

	/*
	 * Get the smallest LSN committed by quorum
	 */
	return responses[n_safekeepers - quorum];
}

static void
HandleSafekeeperResponse(void)
{
	XLogRecPtr	minQuorumLsn;
	XLogRecPtr	minFlushLsn;

	minQuorumLsn = GetAcknowledgedByQuorumWALPosition();
	walprop_pg.process_safekeeper_feedback(safekeeper, n_safekeepers, syncSafekeepers, minQuorumLsn);

	/*
	 * Try to advance truncateLsn to minFlushLsn, which is the last record
	 * flushed to all safekeepers. We must always start streaming from the
	 * beginning of the record, which simplifies decoding on the far end.
	 *
	 * Advanced truncateLsn should be not further than nearest commitLsn. This
	 * prevents surprising violation of truncateLsn <= commitLsn invariant
	 * which might occur because 1) truncateLsn can be advanced immediately
	 * once chunk is broadcast to all safekeepers, and commitLsn generally
	 * can't be advanced based on feedback from safekeeper who is still in the
	 * previous epoch (similar to 'leader can't commit entries from previous
	 * term' in Raft); 2) chunks we read from WAL and send are plain sheets of
	 * bytes, but safekeepers ack only on record boundaries.
	 */
	minFlushLsn = CalculateMinFlushLsn();
	if (minFlushLsn > truncateLsn)
	{
		truncateLsn = minFlushLsn;

		/*
		 * Advance the replication slot to free up old WAL files. Note that
		 * slot doesn't exist if we are in syncSafekeepers mode.
		 */
		walprop_pg.confirm_wal_streamed(truncateLsn);
	}

	/*
	 * Generally sync is done when majority switched the epoch so we committed
	 * epochStartLsn and made the majority aware of it, ensuring they are
	 * ready to give all WAL to pageserver. It would mean whichever majority
	 * is alive, there will be at least one safekeeper who is able to stream
	 * WAL to pageserver to make basebackup possible. However, since at the
	 * moment we don't have any good mechanism of defining the healthy and
	 * most advanced safekeeper who should push the wal into pageserver and
	 * basically the random one gets connected, to prevent hanging basebackup
	 * (due to pageserver connecting to not-synced-safekeeper) we currently
	 * wait for all seemingly alive safekeepers to get synced.
	 */
	if (syncSafekeepers)
	{
		int			n_synced;

		n_synced = 0;
		for (int i = 0; i < n_safekeepers; i++)
		{
			Safekeeper *sk = &safekeeper[i];
			bool		synced = sk->appendResponse.commitLsn >= propEpochStartLsn;

			/* alive safekeeper which is not synced yet; wait for it */
			if (sk->state != SS_OFFLINE && !synced)
				return;
			if (synced)
				n_synced++;
		}

		if (n_synced >= quorum)
		{
			/* A quorum of safekeepers has been synced! */
			
			/*
			 * Send empty message to broadcast latest truncateLsn to all safekeepers.
			 * This helps to finish next sync-safekeepers eailier, by skipping recovery
			 * step.
			 * 
			 * We don't need to wait for response because it doesn't affect correctness,
			 * and TCP should be able to deliver the message to safekeepers in case of
			 * network working properly.
			 */
			BroadcastAppendRequest();

			walprop_pg.finish_sync_safekeepers(propEpochStartLsn);
			/* unreachable */
		}
	}
}

/*
 * Try to read CopyData message from i'th safekeeper, resetting connection on
 * failure.
 */
static bool
AsyncRead(Safekeeper *sk, char **buf, int *buf_size)
{
	switch (walprop_pg.conn_async_read(sk->conn, buf, buf_size))
	{
		case PG_ASYNC_READ_SUCCESS:
			return true;

		case PG_ASYNC_READ_TRY_AGAIN:
			/* WL_SOCKET_READABLE is always set during copyboth */
			return false;

		case PG_ASYNC_READ_FAIL:
			elog(WARNING, "Failed to read from node %s:%s in %s state: %s", sk->host,
				 sk->port, FormatSafekeeperState(sk->state),
				 walprop_pg.conn_error_message(sk->conn));
			ShutdownConnection(sk);
			return false;
	}
	Assert(false);
	return false;
}

/*
 * Read next message with known type into provided struct, by reading a CopyData
 * block from the safekeeper's postgres connection, returning whether the read
 * was successful.
 *
 * If the read needs more polling, we return 'false' and keep the state
 * unmodified, waiting until it becomes read-ready to try again. If it fully
 * failed, a warning is emitted and the connection is reset.
 */
static bool
AsyncReadMessage(Safekeeper *sk, AcceptorProposerMessage * anymsg)
{
	char	   *buf;
	int			buf_size;
	uint64		tag;
	StringInfoData s;

	if (!(AsyncRead(sk, &buf, &buf_size)))
		return false;

	/* parse it */
	s.data = buf;
	s.len = buf_size;
	s.cursor = 0;

	tag = pq_getmsgint64_le(&s);
	if (tag != anymsg->tag)
	{
		elog(WARNING, "unexpected message tag %c from node %s:%s in state %s", (char) tag, sk->host,
			 sk->port, FormatSafekeeperState(sk->state));
		ResetConnection(sk);
		return false;
	}
	sk->latestMsgReceivedAt = walprop_pg.get_current_timestamp();
	switch (tag)
	{
		case 'g':
			{
				AcceptorGreeting *msg = (AcceptorGreeting *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->nodeId = pq_getmsgint64_le(&s);
				pq_getmsgend(&s);
				return true;
			}

		case 'v':
			{
				VoteResponse *msg = (VoteResponse *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->voteGiven = pq_getmsgint64_le(&s);
				msg->flushLsn = pq_getmsgint64_le(&s);
				msg->truncateLsn = pq_getmsgint64_le(&s);
				msg->termHistory.n_entries = pq_getmsgint32_le(&s);
				msg->termHistory.entries = palloc(sizeof(TermSwitchEntry) * msg->termHistory.n_entries);
				for (int i = 0; i < msg->termHistory.n_entries; i++)
				{
					msg->termHistory.entries[i].term = pq_getmsgint64_le(&s);
					msg->termHistory.entries[i].lsn = pq_getmsgint64_le(&s);
				}
				msg->timelineStartLsn = pq_getmsgint64_le(&s);
				pq_getmsgend(&s);
				return true;
			}

		case 'a':
			{
				AppendResponse *msg = (AppendResponse *) anymsg;

				msg->term = pq_getmsgint64_le(&s);
				msg->flushLsn = pq_getmsgint64_le(&s);
				msg->commitLsn = pq_getmsgint64_le(&s);
				msg->hs.ts = pq_getmsgint64_le(&s);
				msg->hs.xmin.value = pq_getmsgint64_le(&s);
				msg->hs.catalog_xmin.value = pq_getmsgint64_le(&s);
				if (buf_size > APPENDRESPONSE_FIXEDPART_SIZE)
					ParsePageserverFeedbackMessage(&s, &msg->rf);
				pq_getmsgend(&s);
				return true;
			}

		default:
			{
				Assert(false);
				return false;
			}
	}
}

/*
 * Blocking equivalent to AsyncWrite.
 *
 * We use this everywhere messages are small enough that they should fit in a
 * single packet.
 */
static bool
BlockingWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState success_state)
{
	uint32		events;

	if (!walprop_pg.conn_blocking_write(sk->conn, msg, msg_size))
	{
		elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
			 sk->host, sk->port, FormatSafekeeperState(sk->state),
			 walprop_pg.conn_error_message(sk->conn));
		ShutdownConnection(sk);
		return false;
	}

	sk->state = success_state;

	/*
	 * If the new state will be waiting for events to happen, update the event
	 * set to wait for those
	 */
	events = SafekeeperStateDesiredEvents(success_state);
	if (events)
		walprop_pg.update_event_set(sk, events);

	return true;
}

/*
 * Starts a write into the 'i'th safekeeper's postgres connection, moving to
 * flush_state (adjusting eventset) if write still needs flushing.
 *
 * Returns false if sending is unfinished (requires flushing or conn failed).
 * Upon failure, a warning is emitted and the connection is reset.
 */
static bool
AsyncWrite(Safekeeper *sk, void *msg, size_t msg_size, SafekeeperState flush_state)
{
	switch (walprop_pg.conn_async_write(sk->conn, msg, msg_size))
	{
		case PG_ASYNC_WRITE_SUCCESS:
			return true;
		case PG_ASYNC_WRITE_TRY_FLUSH:

			/*
			 * We still need to call PQflush some more to finish the job; go
			 * to the appropriate state. Update the event set at the bottom of
			 * this function
			 */
			sk->state = flush_state;
			walprop_pg.update_event_set(sk, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
			return false;
		case PG_ASYNC_WRITE_FAIL:
			elog(WARNING, "Failed to send to node %s:%s in %s state: %s",
				 sk->host, sk->port, FormatSafekeeperState(sk->state),
				 walprop_pg.conn_error_message(sk->conn));
			ShutdownConnection(sk);
			return false;
		default:
			Assert(false);
			return false;
	}
}

/*
 * Flushes a previous call to AsyncWrite. This only needs to be called when the
 * socket becomes read or write ready *after* calling AsyncWrite.
 *
 * If flushing successfully completes returns true, otherwise false. Event set
 * is updated only if connection fails, otherwise caller should manually unset
 * WL_SOCKET_WRITEABLE.
 */
static bool
AsyncFlush(Safekeeper *sk)
{
	/*---
	 * PQflush returns:
	 *   0 if successful                    [we're good to move on]
	 *   1 if unable to send everything yet [call PQflush again]
	 *  -1 if it failed                     [emit an error]
	 */
	switch (walprop_pg.conn_flush(sk->conn))
	{
		case 0:
			/* flush is done */
			return true;
		case 1:
			/* Nothing to do; try again when the socket's ready */
			return false;
		case -1:
			elog(WARNING, "Failed to flush write to node %s:%s in %s state: %s",
				 sk->host, sk->port, FormatSafekeeperState(sk->state),
				 walprop_pg.conn_error_message(sk->conn));
			ResetConnection(sk);
			return false;
		default:
			Assert(false);
			return false;
	}
}

static int
CompareLsn(const void *a, const void *b)
{
	XLogRecPtr	lsn1 = *((const XLogRecPtr *) a);
	XLogRecPtr	lsn2 = *((const XLogRecPtr *) b);

	if (lsn1 < lsn2)
		return -1;
	else if (lsn1 == lsn2)
		return 0;
	else
		return 1;
}

/* Returns a human-readable string corresonding to the SafekeeperState
 *
 * The string should not be freed.
 *
 * The strings are intended to be used as a prefix to "state", e.g.:
 *
 *   elog(LOG, "currently in %s state", FormatSafekeeperState(sk->state));
 *
 * If this sort of phrasing doesn't fit the message, instead use something like:
 *
 *   elog(LOG, "currently in state [%s]", FormatSafekeeperState(sk->state));
 */
static char *
FormatSafekeeperState(SafekeeperState state)
{
	char	   *return_val = NULL;

	switch (state)
	{
		case SS_OFFLINE:
			return_val = "offline";
			break;
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			return_val = "connecting";
			break;
		case SS_WAIT_EXEC_RESULT:
			return_val = "receiving query result";
			break;
		case SS_HANDSHAKE_RECV:
			return_val = "handshake (receiving)";
			break;
		case SS_VOTING:
			return_val = "voting";
			break;
		case SS_WAIT_VERDICT:
			return_val = "wait-for-verdict";
			break;
		case SS_SEND_ELECTED_FLUSH:
			return_val = "send-announcement-flush";
			break;
		case SS_IDLE:
			return_val = "idle";
			break;
		case SS_ACTIVE:
			return_val = "active";
			break;
	}

	Assert(return_val != NULL);

	return return_val;
}

/* Asserts that the provided events are expected for given safekeeper's state */
static void
AssertEventsOkForState(uint32 events, Safekeeper *sk)
{
	uint32		expected = SafekeeperStateDesiredEvents(sk->state);

	/*
	 * The events are in-line with what we're expecting, under two conditions:
	 * (a) if we aren't expecting anything, `events` has no read- or
	 * write-ready component. (b) if we are expecting something, there's
	 * overlap (i.e. `events & expected != 0`)
	 */
	bool		events_ok_for_state;	/* long name so the `Assert` is more
										 * clear later */

	if (expected == WL_NO_EVENTS)
		events_ok_for_state = ((events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) == 0);
	else
		events_ok_for_state = ((events & expected) != 0);

	if (!events_ok_for_state)
	{
		/*
		 * To give a descriptive message in the case of failure, we use elog
		 * and then an assertion that's guaranteed to fail.
		 */
		elog(WARNING, "events %s mismatched for safekeeper %s:%s in state [%s]",
			 FormatEvents(events), sk->host, sk->port, FormatSafekeeperState(sk->state));
		Assert(events_ok_for_state);
	}
}

/* Returns the set of events a safekeeper in this state should be waiting on
 *
 * This will return WL_NO_EVENTS (= 0) for some events. */
static uint32
SafekeeperStateDesiredEvents(SafekeeperState state)
{
	uint32		result = WL_NO_EVENTS;

	/* If the state doesn't have a modifier, we can check the base state */
	switch (state)
	{
			/* Connecting states say what they want in the name */
		case SS_CONNECTING_READ:
			result = WL_SOCKET_READABLE;
			break;
		case SS_CONNECTING_WRITE:
			result = WL_SOCKET_WRITEABLE;
			break;

			/* Reading states need the socket to be read-ready to continue */
		case SS_WAIT_EXEC_RESULT:
		case SS_HANDSHAKE_RECV:
		case SS_WAIT_VERDICT:
			result = WL_SOCKET_READABLE;
			break;

			/*
			 * Idle states use read-readiness as a sign that the connection
			 * has been disconnected.
			 */
		case SS_VOTING:
		case SS_IDLE:
			result = WL_SOCKET_READABLE;
			break;

			/*
			 * Flush states require write-ready for flushing. Active state
			 * does both reading and writing.
			 *
			 * TODO: SS_ACTIVE sometimes doesn't need to be write-ready. We
			 * should check sk->flushWrite here to set WL_SOCKET_WRITEABLE.
			 */
		case SS_SEND_ELECTED_FLUSH:
		case SS_ACTIVE:
			result = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
			break;

			/* The offline state expects no events. */
		case SS_OFFLINE:
			result = WL_NO_EVENTS;
			break;

		default:
			Assert(false);
			break;
	}

	return result;
}

/* Returns a human-readable string corresponding to the event set
 *
 * If the events do not correspond to something set as the `events` field of a `WaitEvent`, the
 * returned string may be meaingless.
 *
 * The string should not be freed. It should also not be expected to remain the same between
 * function calls. */
static char *
FormatEvents(uint32 events)
{
	static char return_str[8];

	/* Helper variable to check if there's extra bits */
	uint32		all_flags = WL_LATCH_SET
	| WL_SOCKET_READABLE
	| WL_SOCKET_WRITEABLE
	| WL_TIMEOUT
	| WL_POSTMASTER_DEATH
	| WL_EXIT_ON_PM_DEATH
	| WL_SOCKET_CONNECTED;

	/*
	 * The formatting here isn't supposed to be *particularly* useful -- it's
	 * just to give an sense of what events have been triggered without
	 * needing to remember your powers of two.
	 */

	return_str[0] = (events & WL_LATCH_SET) ? 'L' : '_';
	return_str[1] = (events & WL_SOCKET_READABLE) ? 'R' : '_';
	return_str[2] = (events & WL_SOCKET_WRITEABLE) ? 'W' : '_';
	return_str[3] = (events & WL_TIMEOUT) ? 'T' : '_';
	return_str[4] = (events & WL_POSTMASTER_DEATH) ? 'D' : '_';
	return_str[5] = (events & WL_EXIT_ON_PM_DEATH) ? 'E' : '_';
	return_str[5] = (events & WL_SOCKET_CONNECTED) ? 'C' : '_';

	if (events & (~all_flags))
	{
		elog(WARNING, "Event formatting found unexpected component %d",
			 events & (~all_flags));
		return_str[6] = '*';
		return_str[7] = '\0';
	}
	else
		return_str[6] = '\0';

	return (char *) &return_str;
}
