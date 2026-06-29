/*
 * ubus-wsd -- WebSocket transport daemon for ubus JSON-RPC.
 *
 * Standalone daemon (NOT a uhttpd plugin -- uhttpd's plugin loader is
 * hardcoded to three internal plugins and doesn't accept third parties).
 * We listen on our own TLS port via libwebsockets, authenticate via
 * rpcd's session.access, and dispatch to ubus.
 *
 * Endpoint:     wss://<host>:<port>/ubus-ws where <port> comes from
 *               /etc/config/ubus-ws (default 8443).
 *
 * Wire:         JSON-RPC 2.0 mirroring /ubus:
 *                 call/list/subscribe/unsubscribe (id, params)
 *                 reply (id, result | error)
 *                 notify (no id, params: [path, type, data])
 *
 * Auth:         Sec-WebSocket-Protocol: ubus-json-rpc-v1, bearer.<sid>
 *               (primary; only way browser WebSocket constructors can pass
 *                auth) OR Authorization: Bearer <sid> (curl/scripts).
 *               Sid validated against rpcd session.access at upgrade,
 *               re-checked per-op at dispatch.
 *
 * UAF rule:     Every per-connection ubus_subscriber is unregistered from
 *               LWS_CALLBACK_CLOSED. The mod-ubus SSE path forgot this and
 *               crashes ~30s after disconnect. Don't replicate.
 *
 * DoS posture: libwebsockets enforces frame-size and UTF-8 limits; we
 *              still cap:
 *                - per-conn subscriptions: 64 (-32005 on overflow)
 *                - tx backlog: 1 MiB queued (close 1009)
 *                - max single frame: 1 MiB (via lws rx_buffer_size)
 *
 * Copyright (C) 2026  Apache-2.0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <libubox/blobmsg.h>
#include <libubox/blobmsg_json.h>
#include <libubox/list.h>
#include <libubox/avl.h>
#include <libubox/avl-cmp.h>
#include <libubox/uloop.h>

#include <libubus.h>
#include <json-c/json.h>
#include <libwebsockets.h>

#include "ws.h"

/* ---- constants ---------------------------------------------------------- */

#define WS_SUBPROTO          "ubus-json-rpc-v1"
#define WS_BEARER_TAG        "bearer."

#define WS_MAX_SUBS_DEFAULT  64
#define WS_TX_BACKLOG_MAX    (1u << 20)         /* 1 MiB queued */
#define WS_MAX_FRAME         (1u << 20)         /* 1 MiB single frame -> rx_buffer_size */

/* JSON-RPC 2.0 reserved error codes + our extensions */
#define RPC_ERR_PARSE        -32700
#define RPC_ERR_INVALID_REQ  -32600
#define RPC_ERR_METHOD       -32601
#define RPC_ERR_PARAMS       -32602
#define RPC_ERR_INTERNAL     -32603
#define RPC_ERR_ACCESS       -32002         /* matches LuCI's index.uc */
#define RPC_ERR_TOO_MANY     -32005         /* our: subscription cap */

/*
 * Max [/{ nesting depth we allow in any inbound JSON. json-c's parser
 * uses recursion for nested containers, so a deeply-nested payload like
 * "[[[...nest N times...]]]" eats N stack frames. With ~8 MB default
 * stack and ~200B per json-c frame, 32 is comfortably under any limit
 * while still allowing realistic payloads (JSON-RPC is rarely nested
 * more than 4-5 levels deep in practice).
 */
#define WS_MAX_JSON_DEPTH    32

/* ---- module-level state ------------------------------------------------- */

static struct ubus_context     *ubus_ctx;
static uint32_t                 session_obj_id;

/* libwebsockets state */
static struct lws_context      *lws_ctx;
static struct lws_vhost        *lws_vh;

/* URL path our protocol responds to */
#define WS_URL_PATH             "/ubus-ws"

/* Cached allowed-origin-hosts array from config (NULL = same-host fallback) */
static const char **allowed_origin_hosts;

/* ---- per-connection state ---------------------------------------------- *
 *
 * Allocated by libwebsockets as per_session_data when the wsi is created.
 * Lifetime: from LWS_CALLBACK_ESTABLISHED through LWS_CALLBACK_CLOSED.
 * Retrieved via the `user` parameter on each callback.
 */
struct ws_conn {
    struct lws       *wsi;          /* set in ESTABLISHED                 */
    char              sid[33];      /* bound session id (32 hex + NUL)    */
    bool              established;  /* gates dispatch                     */

    /* active subscriptions on this connection */
    struct avl_tree   subs;         /* keyed by ubus object path string   */
    unsigned int      num_subs;
    unsigned int      max_subs;     /* default WS_MAX_SUBS_DEFAULT        */

    /* outbound queue drained from LWS_CALLBACK_SERVER_WRITEABLE */
    struct list_head  tx_queue;
    size_t            tx_bytes;     /* total queued payload (backlog cap) */

    /* in-flight async ubus calls -- entries are struct ws_pending_call.
     * teardown detaches them by setting their ->conn to NULL. */
    struct list_head  pending_calls;
};

struct ws_tx_msg {
    struct list_head  list;
    size_t            len;
    /* LWS_PRE pre-padding + payload follows in flex array */
    unsigned char     data[];
};

struct ws_sub {
    struct avl_node        node;
    struct ws_conn        *conn;
    struct ubus_subscriber sub;
    uint32_t               obj_id;
    char                   path[];
};

/*
 * One per in-flight async ubus call. Held in c->pending_calls until the
 * complete_cb fires. If the WS connection tears down while a call is
 * in-flight, ws_conn_teardown() sets ->conn=NULL so the complete_cb
 * knows to drop the reply rather than write to a freed connection.
 */
struct ws_pending_call {
    struct list_head      link;        /* on c->pending_calls            */
    struct ws_conn       *conn;        /* NULL == conn torn down -- drop */
    struct json_object   *id;          /* owned ref to JSON-RPC id       */
    struct json_object   *out;         /* set by data_cb, drained by cb  */
    struct ubus_request   req;
};

/* ---- forward decls ----------------------------------------------------- */

static int   ws_lws_cb(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len);
static int   ws_queue_text(struct ws_conn *c, const char *s);
static int   ws_handle_jsonrpc(struct ws_conn *c, const char *json, size_t len);
static int   ws_notify_cb(struct ubus_context *ctx, struct ubus_object *obj,
                          struct ubus_request_data *req, const char *method,
                          struct blob_attr *msg);
static void  ws_conn_teardown(struct ws_conn *c);

/* ---- header / auth helpers -------------------------------------------- */

/*
 * Copy a header value identified by a libwebsockets token into `buf`. Returns
 * the length on success (without trailing NUL), or -1 if the header is
 * missing or wouldn't fit in `buflen`. The result is always NUL-terminated
 * on success.
 *
 * Tokens commonly used: WSI_TOKEN_HOST, WSI_TOKEN_ORIGIN,
 * WSI_TOKEN_HTTP_AUTHORIZATION, WSI_TOKEN_PROTOCOL (Sec-WebSocket-Protocol).
 */
static int
get_hdr(struct lws *wsi, enum lws_token_indexes tok, char *buf, size_t buflen)
{
    int len = lws_hdr_total_length(wsi, tok);
    if (len <= 0 || (size_t)len >= buflen) return -1;
    if (lws_hdr_copy(wsi, buf, buflen, tok) < 0) return -1;
    buf[len] = '\0';
    return len;
}

/*
 * Return the rpcd "session" object id, looking it up on demand if the
 * cached value is stale. session_obj_id is module-static and zero-init
 * via BSS; if the lookup at plugin init failed (rpcd not yet running),
 * we retry on each auth/ACL call so the plugin recovers once rpcd is up.
 */
static bool
get_session_obj_id(uint32_t *out)
{
    if (session_obj_id != 0) { *out = session_obj_id; return true; }
    if (ubus_lookup_id(ubus_ctx, "session", &session_obj_id) == 0) {
        *out = session_obj_id;
        return true;
    }
    return false;
}

/*
 * Pre-flight depth check for inbound JSON. Walks the bytes once counting
 * { and [, respecting "..." string boundaries and \ escapes. Returns
 * false if nesting depth would exceed max_depth at any point. Cheap
 * (linear scan, no allocation) -- runs before json-c's recursive parse.
 */
static bool
json_depth_ok(const char *s, size_t len, int max_depth)
{
    int depth = 0;
    bool in_string = false, escape = false;
    size_t i;

    for (i = 0; i < len; i++) {
        char c = s[i];
        if (escape)   { escape = false; continue; }
        if (in_string) {
            if      (c == '\\') escape = true;
            else if (c == '"')  in_string = false;
            continue;
        }
        if      (c == '"')                in_string = true;
        else if (c == '[' || c == '{')    { if (++depth > max_depth) return false; }
        else if (c == ']' || c == '}')    { if (--depth < 0)         return false; }
    }
    return true;
}

/* Used by both auth paths so they enforce the same 32-hex-char shape. */
static bool
is_32hex(const char *s)
{
    size_t i;
    for (i = 0; i < 32; i++)
        if (!((s[i] >= '0' && s[i] <= '9') ||
              (s[i] >= 'a' && s[i] <= 'f') ||
              (s[i] >= 'A' && s[i] <= 'F')))
            return false;
    return true;
}

/*
 * Extract a 32-hex sid from a Sec-WebSocket-Protocol header value of the
 * form "ubus-json-rpc-v1, bearer.<32 hex chars>". Returns pointer into
 * the header value (NOT NUL-terminated at sid end).
 */
static const char *
find_sid_in_subprotocol(const char *hdr)
{
    const char *p = hdr;
    size_t tag_len = strlen(WS_BEARER_TAG);

    while (p && *p) {
        while (*p == ' ' || *p == ',') p++;
        if (!strncasecmp(p, WS_BEARER_TAG, tag_len) &&
            strlen(p + tag_len) >= 32 &&
            is_32hex(p + tag_len))
            return p + tag_len;
        p = strchr(p, ',');
    }
    return NULL;
}

static bool
ws_authenticate(struct lws *wsi, char out_sid[33])
{
    char swp_buf[512], auth_buf[256];
    const char *sid_src = NULL;
    const char *swp  = (get_hdr(wsi, WSI_TOKEN_PROTOCOL,
                                swp_buf,  sizeof(swp_buf))  > 0) ? swp_buf  : NULL;
    const char *auth = (get_hdr(wsi, WSI_TOKEN_HTTP_AUTHORIZATION,
                                auth_buf, sizeof(auth_buf)) > 0) ? auth_buf : NULL;
    struct blob_buf req = {};
    int err;

    if (swp) sid_src = find_sid_in_subprotocol(swp);

    if (!sid_src && auth && !strncasecmp(auth, "Bearer ", 7)) {
        const char *s = auth + 7;
        while (*s == ' ') s++;
        /* Enforce 32-hex-char shape (same rule as the SWP path) -- a
         * non-hex 32-char string would otherwise be passed to rpcd which
         * would reject it, but checking here avoids the round-trip and
         * keeps the two paths consistent. */
        if (strlen(s) >= 32 && is_32hex(s)) sid_src = s;
    }

    if (!sid_src) return false;

    memcpy(out_sid, sid_src, 32);
    out_sid[32] = '\0';

    /* Resolve "session" object id (with lazy re-lookup if rpcd wasn't
     * running at plugin init). */
    uint32_t sid_oid;
    if (!get_session_obj_id(&sid_oid)) return false;

    /* Probe session.access. We only accept err == 0 -- previously we also
     * accepted UBUS_STATUS_PERMISSION_DENIED on the assumption that "any
     * response from rpcd means the session exists", but expired/invalid
     * sessions can also produce PERMISSION_DENIED, which let zombie WS
     * connections through. Tightening to err == 0 only. */
    blob_buf_init(&req, 0);
    blobmsg_add_string(&req, "ubus_rpc_session", out_sid);
    blobmsg_add_string(&req, "scope", "ubus");
    blobmsg_add_string(&req, "object", "session");
    blobmsg_add_string(&req, "function", "access");

    err = ubus_invoke(ubus_ctx, sid_oid, "access",
                      req.head, NULL, NULL, 1000);
    blob_buf_free(&req);

    return err == 0;
}

static void
ws_acl_reply_cb(struct ubus_request *r, int type, struct blob_attr *msg)
{
    bool *allowed = r->priv;
    struct blob_attr *cur;
    int rem;

    (void)type;

    blob_for_each_attr(cur, msg, rem)
        if (blobmsg_type(cur) == BLOBMSG_TYPE_BOOL &&
            !strcmp(blobmsg_name(cur), "access"))
            *allowed = blobmsg_get_bool(cur);
}

static bool
ws_acl_check(const char *sid, const char *obj, const char *fn)
{
    struct blob_buf req = {};
    bool allowed = false;
    int err;
    uint32_t sid_oid;

    if (!get_session_obj_id(&sid_oid)) return false;

    blob_buf_init(&req, 0);
    blobmsg_add_string(&req, "ubus_rpc_session", sid);
    blobmsg_add_string(&req, "scope", "ubus");
    blobmsg_add_string(&req, "object", obj);
    blobmsg_add_string(&req, "function", fn);

    err = ubus_invoke(ubus_ctx, sid_oid, "access",
                      req.head, ws_acl_reply_cb, &allowed, 1000);
    blob_buf_free(&req);

    return (err == 0) && allowed;
}

/* ---- Origin validation (CSWSH defense in depth) ----------------------- *
 *
 * Policy: extract the bare hostname from Origin and compare to either
 *   (a) the bare hostname from the Host header (default permissive mode --
 *       "same-host any-port"), OR
 *   (b) any entry in the UCI allowed_origin_hosts allowlist (strict mode,
 *       opt-in by configuring the list).
 *
 * Port differences are tolerated because typical deployments run LuCI
 * on port 443 and us on port 8443 -- same host, different ports.
 *
 * IPv6 host literals (`[::1]:8443` / `[::1]`) are stripped of brackets.
 * Plain hostname/IPv4 ("router.lan", "192.168.1.1") flows through.
 */

/* Pull the bare hostname out of "scheme://host[:port]/path..." */
static const char *
hostname_from_origin(const char *origin, char *buf, size_t buflen)
{
    const char *p, *end;
    size_t n;

    p = strstr(origin, "://");
    if (!p) return NULL;
    p += 3;

    if (*p == '[') {                           /* IPv6 [::1]:port form */
        end = strchr(p, ']');
        if (!end) return NULL;
        n = (size_t)(end - p - 1);
        if (n == 0 || n >= buflen) return NULL;
        memcpy(buf, p + 1, n);
        buf[n] = '\0';
        return buf;
    }

    end = p;
    while (*end && *end != ':' && *end != '/') end++;
    n = (size_t)(end - p);
    if (n == 0 || n >= buflen) return NULL;
    memcpy(buf, p, n);
    buf[n] = '\0';
    return buf;
}

/* Pull the bare hostname out of a Host header value "host[:port]" */
static const char *
hostname_from_host(const char *host, char *buf, size_t buflen)
{
    const char *end;
    size_t n;

    if (host[0] == '[') {                      /* IPv6 [::1]:port form */
        end = strchr(host, ']');
        if (!end) return NULL;
        n = (size_t)(end - host - 1);
        if (n == 0 || n >= buflen) return NULL;
        memcpy(buf, host + 1, n);
        buf[n] = '\0';
        return buf;
    }

    end = strchr(host, ':');
    n = end ? (size_t)(end - host) : strlen(host);
    if (n == 0 || n >= buflen) return NULL;
    memcpy(buf, host, n);
    buf[n] = '\0';
    return buf;
}

static bool
ws_origin_ok(struct lws *wsi)
{
    char origin_buf[256], host_buf[128];
    char origin_host[128], host_host[128];
    size_t i;

    /* Origin absent: allow. Non-browser clients (curl, scripts) don't send
     * it; Bearer auth still gates access. */
    if (get_hdr(wsi, WSI_TOKEN_ORIGIN, origin_buf, sizeof(origin_buf)) <= 0)
        return true;

    if (!hostname_from_origin(origin_buf, origin_host, sizeof(origin_host)))
        return false;

    /* Strict mode: UCI allowed_origin_hosts is the only authority. */
    if (allowed_origin_hosts && allowed_origin_hosts[0]) {
        for (i = 0; allowed_origin_hosts[i]; i++)
            if (!strcasecmp(origin_host, allowed_origin_hosts[i]))
                return true;
        return false;
    }

    /* Permissive mode (default): Origin's hostname must equal Host's. */
    if (get_hdr(wsi, WSI_TOKEN_HOST, host_buf, sizeof(host_buf)) <= 0)
        return false;
    if (!hostname_from_host(host_buf, host_host, sizeof(host_host)))
        return false;

    return strcasecmp(origin_host, host_host) == 0;
}

/* ---- tx queue --------------------------------------------------------- */

static int
ws_queue_text(struct ws_conn *c, const char *s)
{
    size_t len = strlen(s);
    struct ws_tx_msg *m;

    if (c->tx_bytes + len > WS_TX_BACKLOG_MAX) {
        /* Tear down the connection -- slow consumer */
        if (c->wsi)
            lws_set_timeout(c->wsi, PENDING_TIMEOUT_USER_OK, LWS_TO_KILL_ASYNC);
        return -1;
    }

    m = malloc(sizeof(*m) + LWS_PRE + len);
    if (!m) return -1;

    m->len = len;
    memcpy(m->data + LWS_PRE, s, len);
    list_add_tail(&m->list, &c->tx_queue);
    c->tx_bytes += len;

    if (c->wsi)
        lws_callback_on_writable(c->wsi);
    return 0;
}

/* ---- JSON-RPC dispatch ------------------------------------------------ */

static void
ws_send_error(struct ws_conn *c, struct json_object *id, int code, const char *msg)
{
    struct json_object *r = json_object_new_object();
    struct json_object *e = json_object_new_object();
    json_object_object_add(r, "jsonrpc", json_object_new_string("2.0"));
    if (id) json_object_object_add(r, "id", json_object_get(id));
    json_object_object_add(e, "code", json_object_new_int(code));
    json_object_object_add(e, "message", json_object_new_string(msg));
    json_object_object_add(r, "error", e);
    ws_queue_text(c, json_object_to_json_string(r));
    json_object_put(r);
}

static void
ws_send_result(struct ws_conn *c, struct json_object *id, struct json_object *result)
{
    struct json_object *r = json_object_new_object();
    json_object_object_add(r, "jsonrpc", json_object_new_string("2.0"));
    if (id) json_object_object_add(r, "id", json_object_get(id));
    json_object_object_add(r, "result", result);
    ws_queue_text(c, json_object_to_json_string(r));
    json_object_put(r);
}

/*
 * Async ubus_invoke callbacks. data_cb fires once per reply chunk and
 * stashes the parsed JSON; complete_cb fires once when the call ends
 * (success, error, or timeout) and is the only place we touch the WS
 * connection. If conn has been torn down (->conn == NULL), we drop the
 * reply silently.
 */
static void
ws_call_data_cb(struct ubus_request *r, int type, struct blob_attr *msg)
{
    struct ws_pending_call *pc = container_of(r, struct ws_pending_call, req);
    char *s = blobmsg_format_json(msg, true);

    (void)type;

    /* If multiple data chunks arrive, the last one wins; ubus calls typically
     * have a single reply blob, but be defensive about repeat callbacks. */
    if (pc->out) json_object_put(pc->out);
    pc->out = s ? json_tokener_parse(s) : NULL;
    free(s);
}

static void
ws_call_complete_cb(struct ubus_request *r, int status)
{
    struct ws_pending_call *pc = container_of(r, struct ws_pending_call, req);
    struct ws_conn *c = pc->conn;

    list_del(&pc->link);

    if (c) {
        if (status == UBUS_STATUS_OK) {
            struct json_object *result = json_object_new_array();
            json_object_array_add(result, json_object_new_int(0));
            if (pc->out) {
                json_object_array_add(result, pc->out);
                pc->out = NULL;             /* ownership moves into result */
            }
            ws_send_result(c, pc->id, result);
        } else {
            ws_send_error(c, pc->id, -32000 - status, ubus_strerror(status));
        }
    }
    /* If c == NULL the connection died while we were in flight; just drop
     * everything. ws_send_* would write to a freed connection otherwise. */

    if (pc->out) json_object_put(pc->out);
    if (pc->id)  json_object_put(pc->id);
    free(pc);
}

static void
ws_dispatch_call(struct ws_conn *c, struct json_object *id, struct json_object *params)
{
    const char *obj, *fn;
    struct json_object *jobj, *jfn, *jargs = NULL;
    struct blob_buf req = {};
    uint32_t oid;
    int err;
    struct ws_pending_call *pc;

    if (!params || json_object_array_length(params) < 3) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    jobj = json_object_array_get_idx(params, 0);
    jfn  = json_object_array_get_idx(params, 1);
    jargs= json_object_array_get_idx(params, 2);
    if (!jobj || !jfn ||
        !json_object_is_type(jobj, json_type_string) ||
        !json_object_is_type(jfn,  json_type_string)) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    obj = json_object_get_string(jobj);
    fn  = json_object_get_string(jfn);

    /* Unify "not found" and "no access" responses so an unauthenticated
     * caller can't probe which ubus objects exist on the device. */
    if (!ws_acl_check(c->sid, obj, fn) ||
        ubus_lookup_id(ubus_ctx, obj, &oid)) {
        ws_send_error(c, id, RPC_ERR_ACCESS, "Access denied"); return;
    }

    blob_buf_init(&req, 0);
    if (jargs && json_object_is_type(jargs, json_type_object))
        blobmsg_add_object(&req, jargs);
    blobmsg_add_string(&req, "ubus_rpc_session", c->sid);

    pc = calloc(1, sizeof(*pc));
    if (!pc) {
        blob_buf_free(&req);
        ws_send_error(c, id, RPC_ERR_INTERNAL, "OOM"); return;
    }
    pc->conn = c;
    pc->id   = id ? json_object_get(id) : NULL;     /* take ref */

    err = ubus_invoke_async(ubus_ctx, oid, fn, req.head, &pc->req);
    blob_buf_free(&req);

    if (err) {
        if (pc->id) json_object_put(pc->id);
        free(pc);
        ws_send_error(c, id, -32000 - err, ubus_strerror(err));
        return;
    }

    pc->req.data_cb     = ws_call_data_cb;
    pc->req.complete_cb = ws_call_complete_cb;
    list_add_tail(&pc->link, &c->pending_calls);

    /* Fire and forget -- complete_cb sends the reply when ubusd answers
     * or the request times out. uhttpd's uloop stays responsive. */
    ubus_complete_request_async(ubus_ctx, &pc->req);
}

static void
ws_dispatch_list(struct ws_conn *c, struct json_object *id, struct json_object *params)
{
    /* TODO(verify on-device): ubus_lookup walk to populate {name: {method:
     * {param: type}}}. Stub returns empty object so callers see the verb
     * is supported. Implement to mirror index.uc:76-100 list semantics. */
    (void)params;
    ws_send_result(c, id, json_object_new_object());
}

static void
ws_dispatch_subscribe(struct ws_conn *c, struct json_object *id,
                      struct json_object *params)
{
    const char *path;
    struct json_object *jpath;
    struct ws_sub *s;
    uint32_t oid;
    int err;

    if (!params || json_object_array_length(params) < 1) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    jpath = json_object_array_get_idx(params, 0);
    if (!jpath || !json_object_is_type(jpath, json_type_string)) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    path = json_object_get_string(jpath);

    /* idempotent */
    if (avl_find(&c->subs, path)) {
        ws_send_result(c, id, json_object_new_boolean(true));
        return;
    }
    if (c->num_subs >= c->max_subs) {
        ws_send_error(c, id, RPC_ERR_TOO_MANY, "Too many subscriptions"); return;
    }
    /* Unify "not found" and "no access" -- see ws_dispatch_call comment. */
    if (!ws_acl_check(c->sid, path, ":subscribe") ||
        ubus_lookup_id(ubus_ctx, path, &oid)) {
        ws_send_error(c, id, RPC_ERR_ACCESS, "Access denied"); return;
    }

    s = calloc(1, sizeof(*s) + strlen(path) + 1);
    if (!s) { ws_send_error(c, id, RPC_ERR_INTERNAL, "OOM"); return; }
    s->conn = c;
    s->obj_id = oid;
    strcpy(s->path, path);
    s->node.key = s->path;
    s->sub.cb = ws_notify_cb;

    /* Split error paths: only call ubus_unregister_subscriber if register
     * actually succeeded. Unregistering an unregistered subscriber is
     * undefined behaviour in libubus. */
    err = ubus_register_subscriber(ubus_ctx, &s->sub);
    if (err) {
        free(s);
        ws_send_error(c, id, RPC_ERR_INTERNAL, ubus_strerror(err));
        return;
    }
    err = ubus_subscribe(ubus_ctx, &s->sub, oid);
    if (err) {
        ubus_unregister_subscriber(ubus_ctx, &s->sub);
        free(s);
        ws_send_error(c, id, RPC_ERR_INTERNAL, ubus_strerror(err));
        return;
    }

    avl_insert(&c->subs, &s->node);
    c->num_subs++;
    ws_send_result(c, id, json_object_new_boolean(true));
}

static void
ws_dispatch_unsubscribe(struct ws_conn *c, struct json_object *id,
                        struct json_object *params)
{
    const char *path;
    struct json_object *jpath;
    struct ws_sub *s;

    if (!params || json_object_array_length(params) < 1) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    jpath = json_object_array_get_idx(params, 0);
    if (!jpath || !json_object_is_type(jpath, json_type_string)) {
        ws_send_error(c, id, RPC_ERR_PARAMS, "Invalid parameters"); return;
    }
    path = json_object_get_string(jpath);

    s = avl_find_element(&c->subs, path, s, node);
    if (!s) { ws_send_result(c, id, json_object_new_boolean(false)); return; }

    avl_delete(&c->subs, &s->node);
    ubus_unregister_subscriber(ubus_ctx, &s->sub);
    free(s);
    c->num_subs--;

    ws_send_result(c, id, json_object_new_boolean(true));
}

static int
ws_handle_jsonrpc(struct ws_conn *c, const char *json, size_t len)
{
    struct json_tokener *tok = json_tokener_new();
    struct json_object *req, *jver, *jid = NULL, *jmethod, *jparams = NULL;
    const char *method;
    int rv = 0;

    /* Reject pathologically nested JSON before it hits json-c's recursive
     * parser. This bounds stack consumption to ~WS_MAX_JSON_DEPTH frames. */
    if (!json_depth_ok(json, len, WS_MAX_JSON_DEPTH)) {
        ws_send_error(c, NULL, RPC_ERR_PARSE, "Parse error"); rv = -1; goto out;
    }

    req = json_tokener_parse_ex(tok, json, len);
    if (!req || !json_object_is_type(req, json_type_object)) {
        ws_send_error(c, NULL, RPC_ERR_PARSE, "Parse error"); rv = -1; goto out;
    }
    /* Type-check every field before string-comparing it.
     * json_object_object_get_ex only confirms the key EXISTS; the value
     * could be a number/array/whatever. json_object_get_string returns
     * NULL on non-string types, and strcmp(NULL, ...) segfaults. */
    if (!json_object_object_get_ex(req, "jsonrpc", &jver) ||
        !json_object_is_type(jver, json_type_string) ||
        strcmp(json_object_get_string(jver), "2.0") != 0 ||
        !json_object_object_get_ex(req, "method", &jmethod) ||
        !json_object_is_type(jmethod, json_type_string)) {
        ws_send_error(c, NULL, RPC_ERR_INVALID_REQ, "Invalid request");
        rv = -1; goto out;
    }
    json_object_object_get_ex(req, "id", &jid);
    json_object_object_get_ex(req, "params", &jparams);
    method = json_object_get_string(jmethod);

    if      (!strcmp(method, "call"))        ws_dispatch_call(c, jid, jparams);
    else if (!strcmp(method, "list"))        ws_dispatch_list(c, jid, jparams);
    else if (!strcmp(method, "subscribe"))   ws_dispatch_subscribe(c, jid, jparams);
    else if (!strcmp(method, "unsubscribe")) ws_dispatch_unsubscribe(c, jid, jparams);
    else ws_send_error(c, jid, RPC_ERR_METHOD, "Method not found");

out:
    if (req) json_object_put(req);
    json_tokener_free(tok);
    return rv;
}

/* ---- notify -> queue frame -------------------------------------------- */

static int
ws_notify_cb(struct ubus_context *ctx, struct ubus_object *obj,
             struct ubus_request_data *req, const char *method,
             struct blob_attr *msg)
{
    struct ubus_subscriber *sub = container_of(obj, struct ubus_subscriber, obj);
    struct ws_sub *s = container_of(sub, struct ws_sub, sub);
    struct json_object *frame, *params, *data;
    char *blob_json;

    (void)ctx; (void)req;

    frame  = json_object_new_object();
    params = json_object_new_array();
    json_object_array_add(params, json_object_new_string(s->path));
    json_object_array_add(params, json_object_new_string(method ? method : ""));

    /* Defensive: blob_json may parse to NULL on malformed input. We must
     * never add NULL to a json array -- json-c's behaviour on serialization
     * is undefined and has been observed to crash. Fall back to an empty
     * object so the notify shape is always well-formed. */
    blob_json = msg ? blobmsg_format_json(msg, true) : NULL;
    data = blob_json ? json_tokener_parse(blob_json) : NULL;
    free(blob_json);
    if (!data) data = json_object_new_object();
    json_object_array_add(params, data);

    json_object_object_add(frame, "jsonrpc", json_object_new_string("2.0"));
    json_object_object_add(frame, "method",  json_object_new_string("notify"));
    json_object_object_add(frame, "params",  params);

    ws_queue_text(s->conn, json_object_to_json_string(frame));
    json_object_put(frame);
    return 0;
}

/* ---- THE rule: teardown unregisters every subscriber ------------------ *
 *
 * Called from LWS_CALLBACK_CLOSED. We MUST unregister every ubus_subscriber
 * here -- otherwise ubusd holds a dangling pointer and crashes on the next
 * reaper pass. This is the exact bug mod-ubus SSE has on 25.12.
 */
static void
ws_conn_teardown(struct ws_conn *c)
{
    struct ws_sub *s, *tmp;
    struct ws_tx_msg *m, *mtmp;
    struct ws_pending_call *pc;

    /*
     * If we never reached ESTABLISHED, the list/avl heads are still
     * zero-init and walking them would crash. CLOSED *usually* only fires
     * after ESTABLISHED, but defensive: bail out if nothing was set up.
     */
    if (!c->established) return;

    /*
     * Detach in-flight async ubus calls. We can't safely free them here
     * because ubusd may still call back into them; instead set ->conn=NULL
     * so ws_call_complete_cb knows to drop the reply rather than write to
     * a freed connection. The pc struct itself is freed by its complete_cb
     * when ubusd finally responds (or its 30s timeout fires).
     */
    list_for_each_entry(pc, &c->pending_calls, link)
        pc->conn = NULL;

    avl_for_each_element_safe(&c->subs, s, node, tmp) {
        avl_delete(&c->subs, &s->node);
        ubus_unregister_subscriber(ubus_ctx, &s->sub);
        free(s);
    }

    list_for_each_entry_safe(m, mtmp, &c->tx_queue, list) {
        list_del(&m->list);
        free(m);
    }
    c->tx_bytes = 0;
    c->established = false;
}

/* ---- libwebsockets callback ------------------------------------------- */

static int
ws_lws_cb(struct lws *wsi, enum lws_callback_reasons reason,
          void *user, void *in, size_t len)
{
    struct ws_conn *c = user;

    switch (reason) {

    case LWS_CALLBACK_ESTABLISHED: {
        /* Handshake complete. per_session_data is zero-initialized by lws.
         * Pick up the sid we stashed via lws_set_opaque_user_data() before
         * adopt and copy it into per-session state. */
        char *pending_sid = lws_get_opaque_user_data(wsi);

        c->wsi = wsi;
        avl_init(&c->subs, avl_strcmp, false, NULL);
        c->num_subs = 0;
        c->max_subs = WS_MAX_SUBS_DEFAULT;
        INIT_LIST_HEAD(&c->tx_queue);
        c->tx_bytes = 0;
        INIT_LIST_HEAD(&c->pending_calls);

        if (pending_sid) {
            memcpy(c->sid, pending_sid, 33);
            free(pending_sid);
            lws_set_opaque_user_data(wsi, NULL);
        }
        c->established = true;
        return 0;
    }

    case LWS_CALLBACK_RECEIVE:
        if (!c->established) return -1;
        if (lws_frame_is_binary(wsi)) return -1;        /* TEXT only */
        /* libwebsockets re-assembles fragments and validates UTF-8 (with
         * LWS_SERVER_OPTION_VALIDATE_UTF8). `in`/`len` is the complete
         * message payload. */
        ws_handle_jsonrpc(c, in, len);
        return 0;

    case LWS_CALLBACK_SERVER_WRITEABLE: {
        if (!c->established) return 0;
        while (!list_empty(&c->tx_queue)) {
            struct ws_tx_msg *m = list_first_entry(&c->tx_queue,
                                                   struct ws_tx_msg, list);
            int n = lws_write(wsi, m->data + LWS_PRE, m->len, LWS_WRITE_TEXT);
            if (n < 0) return -1;
            if ((size_t)n < m->len) {
                /* partial write -- shouldn't happen for TEXT but be safe */
                lws_callback_on_writable(wsi);
                return 0;
            }
            list_del(&m->list);
            c->tx_bytes -= m->len;
            free(m);
            if (lws_send_pipe_choked(wsi)) {
                lws_callback_on_writable(wsi);
                break;
            }
        }
        return 0;
    }

    case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION: {
        /*
         * Daemon mode: we ARE the listener now, so this is the gate for
         * the WS upgrade. Three checks:
         *   1. URL must be our configured path (/ubus-ws) -- libwebsockets
         *      doesn't route by URL for WS, just by subprotocol, so we
         *      enforce the path ourselves to reject stray clients.
         *   2. Origin must pass ws_origin_ok (CSWSH defense).
         *   3. Sec-WebSocket-Protocol or Authorization must carry a valid
         *      32-hex sid that rpcd recognizes (ws_authenticate).
         * Stash the resolved sid via lws_set_opaque_user_data for the
         * ESTABLISHED callback to copy into per_session_data.
         */
        char url[128];
        char sid[33];
        char *pending_sid;

        if (get_hdr(wsi, WSI_TOKEN_GET_URI, url, sizeof(url)) <= 0 ||
            strcmp(url, WS_URL_PATH) != 0) {
            fprintf(stderr, "ubus-wsd: rejecting upgrade for URL '%s'\n",
                    get_hdr(wsi, WSI_TOKEN_GET_URI, url, sizeof(url)) > 0 ? url : "?");
            return 1;
        }
        if (!ws_origin_ok(wsi)) {
            fprintf(stderr, "ubus-wsd: rejecting upgrade (Origin policy)\n");
            return 1;
        }
        if (!ws_authenticate(wsi, sid)) {
            fprintf(stderr, "ubus-wsd: rejecting upgrade (auth)\n");
            return 1;
        }

        pending_sid = malloc(33);
        if (!pending_sid) return 1;
        memcpy(pending_sid, sid, 33);
        lws_set_opaque_user_data(wsi, pending_sid);
        return 0;
    }

    case LWS_CALLBACK_CLOSED:
        ws_conn_teardown(c);
        return 0;

    case LWS_CALLBACK_WSI_DESTROY: {
        /* Last-chance cleanup of any pending opaque we never picked up
         * (e.g. client disconnects between adopt and ESTABLISHED). */
        char *pending_sid = lws_get_opaque_user_data(wsi);
        if (pending_sid) {
            free(pending_sid);
            lws_set_opaque_user_data(wsi, NULL);
        }
        return 0;
    }

    default:
        return 0;
    }
}

static const struct lws_protocols ws_protocols[] = {
    {
        .name                  = WS_SUBPROTO,
        .callback              = ws_lws_cb,
        .per_session_data_size = sizeof(struct ws_conn),
        .rx_buffer_size        = WS_MAX_FRAME,
    },
    { NULL, NULL, 0, 0, 0, NULL, 0 }    /* terminator */
};

/* ---- daemon entry / lifecycle ----------------------------------------- */

int
ws_init(const struct ws_config *cfg)
{
    struct lws_context_creation_info info = { 0 };

    /* ubus first -- our auth path needs it before any WS connection arrives.
     * Lazy re-lookup of session_obj_id in get_session_obj_id handles the
     * case where rpcd starts after us. */
    ubus_ctx = ubus_connect(cfg->ubus_socket);
    if (!ubus_ctx) {
        fprintf(stderr, "ubus-wsd: ubus_connect(%s) failed\n",
                cfg->ubus_socket ? cfg->ubus_socket : "default");
        return -1;
    }
    ubus_add_uloop(ubus_ctx);

    if (ubus_lookup_id(ubus_ctx, "session", &session_obj_id))
        fprintf(stderr,
            "ubus-wsd: rpcd 'session' not yet up; will retry on first auth\n");

    /* Origin allowlist is owned by main.c and outlives this call. */
    allowed_origin_hosts = cfg->allowed_origin_hosts;

    /* libwebsockets listening with TLS */
    info.port                     = cfg->port;
    info.protocols                = ws_protocols;
    info.ssl_cert_filepath        = cfg->cert;
    info.ssl_private_key_filepath = cfg->key;
    info.gid                      = -1;
    info.uid                      = -1;
    info.options                  = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT |
                                    LWS_SERVER_OPTION_VALIDATE_UTF8;

    lws_ctx = lws_create_context(&info);
    if (!lws_ctx) {
        fprintf(stderr, "ubus-wsd: lws_create_context failed\n");
        ubus_free(ubus_ctx);
        ubus_ctx = NULL;
        return -1;
    }

    lws_vh = lws_create_vhost(lws_ctx, &info);
    if (!lws_vh) {
        fprintf(stderr, "ubus-wsd: lws_create_vhost failed\n");
        lws_context_destroy(lws_ctx);
        lws_ctx = NULL;
        ubus_free(ubus_ctx);
        ubus_ctx = NULL;
        return -1;
    }

    fprintf(stderr,
        "ubus-wsd: listening on :%d (TLS), endpoint %s\n",
        cfg->port, WS_URL_PATH);
    return 0;
}

void
ws_shutdown(void)
{
    if (lws_ctx) {
        lws_context_destroy(lws_ctx);
        lws_ctx = NULL;
        lws_vh  = NULL;
    }
    if (ubus_ctx) {
        ubus_free(ubus_ctx);
        ubus_ctx = NULL;
    }
    allowed_origin_hosts = NULL;
}
