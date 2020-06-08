/*-------------------------------------------------------------------------
 *
 * pg_recvlogical.c - receive data from a logical decoding slot in a streaming
 *					  fashion and write it to a local file.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/pg_recvlogical.c
 *-------------------------------------------------------------------------
 */
#include "pg_recvlogical/pg_recvlogical.h"
#include "postgres_fe.h"
#include "postgres.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "access/xlog_internal.h"
#include "common/fe_memutils.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "libpq-fe.h"
#include "libpq/pqsignal.h"
#include "pqexpbuffer.h"
#include "receivelog.h"
#include "streamutil.h"

/* Time to sleep between reconnection attempts */
#define RECONNECT_SLEEP_TIME 5

#ifndef EXTENTION_BUILD
#define debug printf
#else
#define debug(...) elog(INFO, __VA_ARGS__)
#endif

/* Global Options */
static int	verbose = 0;
static int	standby_message_timeout = 10 * 1000;	/* 10 sec = default */
static int	fsync_interval = 10 * 1000; /* 10 sec = default */
static XLogRecPtr startpos = InvalidXLogRecPtr;
static XLogRecPtr endpos = InvalidXLogRecPtr;
static bool do_start_slot = false;
static char *replication_slot = NULL;

/* filled pairwise with option, value. value may be NULL */
static char **options;
static size_t noptions = 0;
static const char *plugin = "test_decoding";

/* Global State */
static volatile sig_atomic_t time_to_abort = false;
static TimestampTz output_last_fsync = -1;
static bool output_needs_fsync = false;
static XLogRecPtr output_written_lsn = InvalidXLogRecPtr;
static XLogRecPtr output_fsync_lsn = InvalidXLogRecPtr;

static bool flushAndSendFeedback(PGconn *conn, TimestampTz *now);
static void prepareToTerminate(PGconn *conn, XLogRecPtr endpos,
							   bool keepalive, XLogRecPtr lsn);
/*
 * Send a Standby Status Update message to server.
 */
static bool sendFeedback(PGconn *conn, TimestampTz now, bool force, bool replyRequested)
{
	static XLogRecPtr last_written_lsn = InvalidXLogRecPtr;
	static XLogRecPtr last_fsync_lsn = InvalidXLogRecPtr;

	char		replybuf[1 + 8 + 8 + 8 + 8 + 1];
	int			len = 0;

	/*
	 * we normally don't want to send superfluous feedback, but if it's
	 * because of a timeout we need to, otherwise wal_sender_timeout will kill
	 * us.
	 */
	if (!force &&
		last_written_lsn == output_written_lsn &&
		last_fsync_lsn != output_fsync_lsn)
		return true;

	if (verbose)
		debug( "confirming write up to %X/%X, flush to %X/%X (slot %s)",
					(uint32) (output_written_lsn >> 32), (uint32) output_written_lsn,
					(uint32) (output_fsync_lsn >> 32), (uint32) output_fsync_lsn,
					replication_slot);

	replybuf[len] = 'r';
	len += 1;
	fe_sendint64(output_written_lsn, &replybuf[len]);	/* write */
	len += 8;
	fe_sendint64(output_fsync_lsn, &replybuf[len]); /* flush */
	len += 8;
	fe_sendint64(InvalidXLogRecPtr, &replybuf[len]);	/* apply */
	len += 8;
	fe_sendint64(now, &replybuf[len]);	/* sendTime */
	len += 8;
	replybuf[len] = replyRequested ? 1 : 0; /* replyRequested */
	len += 1;

	startpos = output_written_lsn;
	last_written_lsn = output_written_lsn;
	last_fsync_lsn = output_fsync_lsn;

	if (PQputCopyData(conn, replybuf, len) <= 0 || PQflush(conn))
	{
		debug("could not send feedback packet: %s",
					 PQerrorMessage(conn));
		return false;
	}

	return true;
}

static void
disconnect_atexit(void)
{
	if (conn != NULL)
		PQfinish(conn);
}

static bool
OutputFsync(TimestampTz now)
{
	output_last_fsync = now;

	output_fsync_lsn = output_written_lsn;

	if (fsync_interval <= 0)
		return true;

	if (!output_needs_fsync)
		return true;

	output_needs_fsync = false;

	/* can only fsync if it's a regular file */
	return true;
}

/*
 * Start the log streaming
 */
static void log_streaming(const void* context, pg_recvlogical_on_changes_callback_f on_changes)
{
	PGresult   *res;
	char	   *copybuf = NULL;
	TimestampTz last_status = -1;
	int			i;
	PQExpBuffer query;

	output_written_lsn = InvalidXLogRecPtr;
	output_fsync_lsn = InvalidXLogRecPtr;

	query = createPQExpBuffer();

	/*
	 * Connect in replication mode to the server
	 */
	if (!conn)
	{
		conn = GetConnection();
	}
	if (!conn)
		/* Error message already written in GetConnection() */
		return;

	/*
	 * Start the replication
	 */
	if (verbose)
		debug( "starting log streaming at %X/%X (slot %s)\n",
					(uint32) (startpos >> 32), (uint32) startpos,
					replication_slot);

	/* Initiate the replication stream at specified location */
	appendPQExpBuffer(query, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X",
					  replication_slot, (uint32) (startpos >> 32), (uint32) startpos);

	/* print options if there are any */
	if (noptions)
		appendPQExpBufferStr(query, " (");

	for (i = 0; i < noptions; i++)
	{
		/* separator */
		if (i > 0)
			appendPQExpBufferStr(query, ", ");

		/* write option name */
		appendPQExpBuffer(query, "\"%s\"", options[(i * 2)]);

		/* write option value if specified */
		if (options[(i * 2) + 1] != NULL)
			appendPQExpBuffer(query, " '%s'", options[(i * 2) + 1]);
	}

	if (noptions)
		appendPQExpBufferChar(query, ')');

	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		debug("could not send replication command \"%s\": %s\n",
					 query->data, PQresultErrorMessage(res));
		PQclear(res);
		goto error;
	}
	PQclear(res);

	resetPQExpBuffer(query);

	if (verbose)
		debug( "streaming initiated");

	while (!time_to_abort)
	{
		int			r;
		int			bytes_left;
		int			bytes_written;
		TimestampTz now;
		int			hdr_len;
		XLogRecPtr	cur_record_lsn = InvalidXLogRecPtr;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		/*
		 * Potentially send a status message to the master
		 */
		now = feGetCurrentTimestamp();

		if (feTimestampDifferenceExceeds(output_last_fsync, now,
										 fsync_interval))
		{
			if (!OutputFsync(now))
				goto error;
		}

		if (standby_message_timeout > 0 &&
			feTimestampDifferenceExceeds(last_status, now,
										 standby_message_timeout))
		{
			/* Time to send feedback! */
			if (!sendFeedback(conn, now, true, false))
				goto error;

			last_status = now;
		}

		r = PQgetCopyData(conn, &copybuf, 1);
		if (r == 0)
		{
			/*
			 * In async mode, and no data available. We block on reading but
			 * not more than the specified timeout, so that we can send a
			 * response back to the client.
			 */
			fd_set		input_mask;
			TimestampTz message_target = 0;
			TimestampTz fsync_target = 0;
			struct timeval timeout;
			struct timeval *timeoutptr = NULL;

			if (PQsocket(conn) < 0)
			{
				debug("invalid socket: %s", PQerrorMessage(conn));
				goto error;
			}

			FD_ZERO(&input_mask);
			FD_SET(PQsocket(conn), &input_mask);

			/* Compute when we need to wakeup to send a keepalive message. */
			if (standby_message_timeout)
				message_target = last_status + (standby_message_timeout - 1) *
					((int64) 1000);

			/* Compute when we need to wakeup to fsync the output file. */
			if (fsync_interval > 0 && output_needs_fsync)
				fsync_target = output_last_fsync + (fsync_interval - 1) *
					((int64) 1000);

			/* Now compute when to wakeup. */
			if (message_target > 0 || fsync_target > 0)
			{
				TimestampTz targettime;
				long		secs;
				int			usecs;

				targettime = message_target;

				if (fsync_target > 0 && fsync_target < targettime)
					targettime = fsync_target;

				feTimestampDifference(now,
									  targettime,
									  &secs,
									  &usecs);
				if (secs <= 0)
					timeout.tv_sec = 1; /* Always sleep at least 1 sec */
				else
					timeout.tv_sec = secs;
				timeout.tv_usec = usecs;
				timeoutptr = &timeout;
			}

			r = select(PQsocket(conn) + 1, &input_mask, NULL, NULL, timeoutptr);
			if (r == 0 || (r < 0 && errno == EINTR))
			{
				/*
				 * Got a timeout or signal. Continue the loop and either
				 * deliver a status packet to the server or just go back into
				 * blocking.
				 */
				continue;
			}
			else if (r < 0)
			{
				debug("select() failed: %m");
				goto error;
			}

			/* Else there is actually data on the socket */
			if (PQconsumeInput(conn) == 0)
			{
				debug("could not receive data from WAL stream: %s",
							 PQerrorMessage(conn));
				goto error;
			}
			continue;
		}

		/* End of copy stream */
		if (r == -1)
			break;

		/* Failure while reading the copy stream */
		if (r == -2)
		{
			debug("could not read COPY data: %s",
						 PQerrorMessage(conn));
			goto error;
		}

		/* Check the message type. */
		if (copybuf[0] == 'k')
		{
			int			pos;
			bool		replyRequested;
			XLogRecPtr	walEnd;
			bool		endposReached = false;

			/*
			 * Parse the keepalive message, enclosed in the CopyData message.
			 * We just check if the server requested a reply, and ignore the
			 * rest.
			 */
			pos = 1;			/* skip msgtype 'k' */
			walEnd = fe_recvint64(&copybuf[pos]);
			output_written_lsn = Max(walEnd, output_written_lsn);

			pos += 8;			/* read walEnd */

			pos += 8;			/* skip sendTime */

			if (r < pos + 1)
			{
				debug("streaming header too small: %d", r);
				goto error;
			}
			replyRequested = copybuf[pos];

			if (endpos != InvalidXLogRecPtr && walEnd >= endpos)
			{
				/*
				 * If there's nothing to read on the socket until a keepalive
				 * we know that the server has nothing to send us; and if
				 * walEnd has passed endpos, we know nothing else can have
				 * committed before endpos.  So we can bail out now.
				 */
				endposReached = true;
			}

			/* Send a reply, if necessary */
			if (replyRequested || endposReached)
			{
				if (!flushAndSendFeedback(conn, &now))
					goto error;
				last_status = now;
			}

			if (endposReached)
			{
				prepareToTerminate(conn, endpos, true, InvalidXLogRecPtr);
				time_to_abort = true;
				break;
			}

			continue;
		}
		else if (copybuf[0] != 'w')
		{
			debug("unrecognized streaming header: \"%c\"",
						 copybuf[0]);
			goto error;
		}

		/*
		 * Read the header of the XLogData message, enclosed in the CopyData
		 * message. We only need the WAL location field (dataStart), the rest
		 * of the header is ignored.
		 */
		hdr_len = 1;			/* msgtype 'w' */
		hdr_len += 8;			/* dataStart */
		hdr_len += 8;			/* walEnd */
		hdr_len += 8;			/* sendTime */
		if (r < hdr_len + 1)
		{
			debug("streaming header too small: %d", r);
			goto error;
		}

		/* Extract WAL location for this block */
		cur_record_lsn = fe_recvint64(&copybuf[1]);

		if (endpos != InvalidXLogRecPtr && cur_record_lsn > endpos)
		{
			/*
			 * We've read past our endpoint, so prepare to go away being
			 * cautious about what happens to our output data.
			 */
			if (!flushAndSendFeedback(conn, &now))
				goto error;
			prepareToTerminate(conn, endpos, false, cur_record_lsn);
			time_to_abort = true;
			break;
		}

		output_written_lsn = Max(cur_record_lsn, output_written_lsn);

		bytes_left = r - hdr_len;
		bytes_written = 0;

		/* signal that a fsync is needed */
		output_needs_fsync = true;

		if(on_changes)
		{
			debug(" on_changes initiated: %s\n", copybuf + hdr_len);
			on_changes(context, copybuf + hdr_len, bytes_left);
		}

		if (endpos != InvalidXLogRecPtr && cur_record_lsn == endpos)
		{
			/* endpos was exactly the record we just processed, we're done */
			if (!flushAndSendFeedback(conn, &now))
				goto error;
			prepareToTerminate(conn, endpos, false, cur_record_lsn);
			time_to_abort = true;
			break;
		}
	}

	res = PQgetResult(conn);
	if (PQresultStatus(res) == PGRES_COPY_OUT)
	{
		/*
		 * We're doing a client-initiated clean exit and have sent CopyDone to
		 * the server. We've already sent replay confirmation and fsync'd so
		 * we can just clean up the connection now.
		 */
		goto error;
	}
	else if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		debug("unexpected termination of replication stream: %s",
					 PQresultErrorMessage(res));
		goto error;
	}
	PQclear(res);
error:
	if (copybuf != NULL)
	{
		PQfreemem(copybuf);
		copybuf = NULL;
	}
	destroyPQExpBuffer(query);
	PQfinish(conn);
	conn = NULL;
}

void pg_recvlogical_stream_logical_start(const void* context, pg_recvlogical_on_changes_callback_f on_changes)
{
	/* Stream loop */
	debug( "pg_recvlogical_stream_logical_start");

	while (true)
	{
		log_streaming(context, on_changes);

		if (time_to_abort)
		{
			/*
			 * We've been Ctrl-C'ed or reached an exit limit condition. That's
			 * not an error, so exit without an errorcode.
			 */
			return;
		}
		else
		{
			/* translator: check source for value for %d */
			debug( "disconnected; waiting %d seconds to try again",
						RECONNECT_SLEEP_TIME);
			pg_usleep(RECONNECT_SLEEP_TIME * 1000000);
		}
	}
}

/*
 * Unfortunately we can't do sensible signal handling on windows...
 */
#ifndef WIN32

/*
 * When sigint is called, just tell the system to exit at the next possible
 * moment.
 */
static void
sigint_handler(int signum)
{
	time_to_abort = true;
}

/*
 * Trigger the output file to be reopened.
 */
static void
sighup_handler(int signum)
{
	//output_reopen = true;
}
#endif

static void XloGPositionFromString(const char * xlog)
{
	uint32	hi, lo;

	if (sscanf(xlog, "%X/%X", &hi, &lo) != 2)
	{
		debug("could not parse start position \"%s\"", optarg);
		exit(1);
	}
	startpos = ((uint64) hi) << 32 | lo;
	if (sscanf(optarg, "%X/%X", &hi, &lo) != 2)
	{
		debug("could not parse end position \"%s\"", optarg);
		exit(1);
	}
	endpos = ((uint64) hi) << 32 | lo;

	/*
	if (startpos != InvalidXLogRecPtr && (do_create_slot || do_drop_slot))
	{
		debug("cannot use --create-slot or --drop-slot together with --startpos");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if (endpos != InvalidXLogRecPtr && !do_start_slot)
	{
		debug("--endpos may only be specified with --start");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}
*/
}

static void parseSetOptions(char* data)
{
	char *val = strchr(data, '=');
	if (val != NULL)
	{
		/* remove =; separate data from val */
		*val = '\0';
		val++;
	}
	noptions += 1;
	options = pg_realloc(options, sizeof(char *) * noptions * 2);
	options[(noptions - 1) * 2] = data;
	options[(noptions - 1) * 2 + 1] = val;
}

int
pg_recvlogical_init(const struct pg_recvlogical_init_settings_t* pg_recvlogical_settings, const char* exec_path)
{
	int			c;
	int			option_index;
	uint32		hi,
				lo;
	char	   *db_name;

	if(pg_recvlogical_settings->_connection._password == NULL)
		dbgetpassword = -1;
	else
		dbgetpassword = 1;

	dbname = pg_recvlogical_settings->_connection._dbname;
	dbhost = pg_recvlogical_settings->_connection._host;
	dbport = pg_recvlogical_settings->_connection._port;
	dbuser = pg_recvlogical_settings->_connection._username;
	verbose = pg_recvlogical_settings->_verbose;
	//parseSetOptions();
	//XloGPositionFromString();
	plugin = pg_recvlogical_settings->_repication._plugin;
	replication_slot = pg_recvlogical_settings->_repication._slot;
	standby_message_timeout = pg_recvlogical_settings->_repication._status_interval * 1000;
	verbose = 1;
	/*
	 * Required arguments
	 */
	if (replication_slot == NULL)
	{
		debug("no slot specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

#ifndef WIN32
	pqsignal(SIGINT, sigint_handler);
	pqsignal(SIGHUP, sighup_handler);
#endif

	/*
	 * Obtain a connection to server. This is not really necessary but it
	 * helps to get more precise error messages about authentication, required
	 * GUC parameters and such.
	 */
	conn = GetConnection();
	if (!conn)
		/* Error message already written in GetConnection() */
		exit(1);
	atexit(disconnect_atexit);

	/*
	 * Run IDENTIFY_SYSTEM to make sure we connected using a database specific
	 * replication connection.
	 */
	if (!RunIdentifySystem(conn, NULL, NULL, NULL, &db_name))
		exit(1);

	if (db_name == NULL)
	{
		debug("could not establish database-specific replication connection");
		exit(1);
	}

	debug( "pg_recvlogical_init");
	/*
	 * Set umask so that directories/files are created with the same
	 * permissions as directories/files in the source data directory.
	 *
	 * pg_mode_mask is set to owner-only by default and then updated in
	 * GetConnection() where we get the mode from the server-side with
	 * RetrieveDataDirCreatePerm() and then call SetDataDirectoryCreatePerm().
	 */
	umask(pg_mode_mask);

	return 0;
}

/*
 * Fsync our output data, and send a feedback message to the server.  Returns
 * true if successful, false otherwise.
 *
 * If successful, *now is updated to the current timestamp just before sending
 * feedback.
 */
static bool
flushAndSendFeedback(PGconn *conn, TimestampTz *now)
{
	/* flush data to disk, so that we send a recent flush pointer */
	if (!OutputFsync(*now))
		return false;
	*now = feGetCurrentTimestamp();
	if (!sendFeedback(conn, *now, true, false))
		return false;

	return true;
}

/*
 * Try to inform the server about our upcoming demise, but don't wait around or
 * retry on failure.
 */
static void
prepareToTerminate(PGconn *conn, XLogRecPtr endpos, bool keepalive, XLogRecPtr lsn)
{
	(void) PQputCopyEnd(conn, NULL);
	(void) PQflush(conn);

	if (verbose)
	{
		if (keepalive)
			debug( "end position %X/%X reached by keepalive",
						(uint32) (endpos >> 32), (uint32) endpos);
		else
			debug( "end position %X/%X reached by WAL record at %X/%X",
						(uint32) (endpos >> 32), (uint32) (endpos),
						(uint32) (lsn >> 32), (uint32) lsn);
	}
}
