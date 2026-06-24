/*
 * ubus-wsd -- WebSocket transport daemon for ubus JSON-RPC.
 *
 * Public interface between main.c (process bootstrap, UCI config, signals)
 * and ws.c (libwebsockets vhost + ubus + JSON-RPC dispatch).
 *
 * Copyright (C) 2026  Apache-2.0
 */

#ifndef UBUS_WSD_WS_H
#define UBUS_WSD_WS_H

#include <stdbool.h>

/*
 * Daemon configuration. Populated by main.c from /etc/config/ubus-ws via
 * libuci, then handed to ws_init() once at startup.
 *
 * String fields are owned by main.c -- ws.c may keep pointers to them for
 * the daemon's lifetime, but does NOT free them.
 */
struct ws_config {
	int          port;                  /* TLS listen port, default 8443    */
	const char  *cert;                  /* PEM cert file path               */
	const char  *key;                   /* PEM private key file path        */
	const char  *ubus_socket;           /* NULL = libubus default           */

	/*
	 * Origin allowlist (hostnames only -- ports ignored). If NULL/empty,
	 * the daemon falls back to "same hostname as the Host: header" -- the
	 * default permissive posture for routers where LuCI lives on the same
	 * host but different port. Otherwise must contain an explicit list and
	 * Origin's hostname is compared against each entry case-insensitively.
	 */
	const char **allowed_origin_hosts;  /* NULL-terminated array, or NULL   */
};

/*
 * Start the WS daemon: open ubus connection, create libwebsockets context
 * listening on cfg->port with TLS using cfg->cert/cfg->key. Returns 0 on
 * success, -1 on any setup error (already logged). Caller must have done
 * uloop_init() before calling this.
 */
int  ws_init(const struct ws_config *cfg);

/*
 * Tear down the daemon: destroy lws_context, close ubus connection,
 * cancel any in-flight requests. Safe to call multiple times.
 */
void ws_shutdown(void);

#endif /* UBUS_WSD_WS_H */
