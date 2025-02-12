#include <sys/resource.h>

#ifndef WALPROPOSER_LIB
#include <curl/curl.h>
#endif

#include "postgres.h"

#include "neon_utils.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

/*
 * Convert a character which represents a hexadecimal digit to an integer.
 *
 * Returns -1 if the character is not a hexadecimal digit.
 */
static int
HexDecodeChar(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;

	return -1;
}

/*
 * Decode a hex string into a byte string, 2 hex chars per byte.
 *
 * Returns false if invalid characters are encountered; otherwise true.
 */
bool
HexDecodeString(uint8 *result, char *input, int nbytes)
{
	int			i;

	for (i = 0; i < nbytes; ++i)
	{
		int			n1 = HexDecodeChar(input[i * 2]);
		int			n2 = HexDecodeChar(input[i * 2 + 1]);

		if (n1 < 0 || n2 < 0)
			return false;
		result[i] = n1 * 16 + n2;
	}

	return true;
}

/* --------------------------------
 *		pq_getmsgint16	- get a binary 2-byte int from a message buffer
 * --------------------------------
 */
uint16
pq_getmsgint16(StringInfo msg)
{
	return pq_getmsgint(msg, 2);
}

/* --------------------------------
 *		pq_getmsgint32	- get a binary 4-byte int from a message buffer
 * --------------------------------
 */
uint32
pq_getmsgint32(StringInfo msg)
{
	return pq_getmsgint(msg, 4);
}

/* --------------------------------
 *		pq_getmsgint32_le	- get a binary 4-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint32
pq_getmsgint32_le(StringInfo msg)
{
	uint32		n32;

	pq_copymsgbytes(msg, (char *) &n32, sizeof(n32));

	return n32;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint64
pq_getmsgint64_le(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return n64;
}

/* append a binary [u]int32 to a StringInfo buffer in native (LE) order */
void
pq_sendint32_le(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	memcpy(buf->data + buf->len, &i, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/* append a binary [u]int64 to a StringInfo buffer in native (LE) order */
void
pq_sendint64_le(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	memcpy(buf->data + buf->len, &i, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/*
 * Disables core dump for the current process.
 */
void
disable_core_dump()
{
	struct rlimit rlim;

#ifdef WALPROPOSER_LIB			/* skip in simulation mode */
	return;
#endif

	rlim.rlim_cur = 0;
	rlim.rlim_max = 0;
	if (setrlimit(RLIMIT_CORE, &rlim))
	{
		int			save_errno = errno;

		fprintf(stderr, "WARNING: disable cores setrlimit failed: %s", strerror(save_errno));
	}
}

#ifndef WALPROPOSER_LIB

/*
 * On macOS with a libcurl that has IPv6 support, curl_global_init() calls
 * SCDynamicStoreCopyProxies(), which makes the program multithreaded. An ideal
 * place to call curl_global_init() would be _PG_init(), but Neon has to be
 * added to shared_preload_libraries, which are loaded in the Postmaster
 * process. The Postmaster is not supposed to become multithreaded at any point
 * in its lifecycle. Postgres doesn't have any good hook that I know of to
 * initialize per-backend structures, so we have to check this on any
 * allocation of a CURL handle.
 *
 * Free the allocated CURL handle with curl_easy_cleanup(3).
 *
 * https://developer.apple.com/documentation/systemconfiguration/1517088-scdynamicstorecopyproxies
 */
CURL *
alloc_curl_handle(void)
{
	static bool curl_initialized = false;

	CURL *handle;

	if (unlikely(!curl_initialized))
	{
		/* Protected by mutex internally */
		if (curl_global_init(CURL_GLOBAL_DEFAULT))
		{
			elog(ERROR, "Failed to initialize curl");
		}

		curl_initialized = true;
	}

	handle = curl_easy_init();
	if (handle == NULL)
	{
		elog(ERROR, "Failed to initialize curl handle");
	}

	return handle;
}

#endif
