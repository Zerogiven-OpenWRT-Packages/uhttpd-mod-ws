/*
 * uhttpd-mod-ws -- RFC 6455 WebSocket transport for ubus JSON-RPC
 *
 * Plugin model:  uhttpd .so plugin. uhttpd owns the listening socket(s),
 *                TLS termination, and uloop. We dispatch on /<ubus_prefix>-ws,
 *                hand the upgraded socket to libwebsockets via
 *                lws_adopt_socket_readbuf(), and from there libwebsockets
 *                handles the RFC 6455 protocol (handshake, framing, masking,
 *                control frames, UTF-8 validation, close codes).
 *
 * Why libwebsockets:
 *   We replaced ~600 LOC of hand-rolled WS code (frame parser, mask
 *   handling, opcode dispatch, handshake hash, frame writer) with calls
 *   into libwebsockets. The deleted code was the security-sensitive part:
 *   audited library > brand-new C parser, especially on a router host.
 *   OpenWrt builds libwebsockets with LWS_WITH_ULOOP=ON so the library
 *   shares uhttpd's existing uloop -- no event-loop bridge needed.
 *
 * Endpoint:     <ubus_prefix>-ws (e.g. /ubus-ws). Sibling of /ubus, NOT a
 *               child -- mod-ubus's check_url would intercept /ubus/ws.
 *               If c->ubus_prefix is empty, we don't register.
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

#include "uhttpd.h"
#include "plugin.h"

/* ---- constants ---------------------------------------------------------- */

#define WS_SUFFIX            "-ws"
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

/* ---- module-level state ------------------------------------------------- */

static const struct uhttpd_ops *ops;
static struct config           *_conf;
static struct ubus_context     *ubus_ctx;
static uint32_t                 session_obj_id;

/* runtime-derived endpoint prefix: "<c->ubus_prefix>-ws" */
static char     runtime_prefix[128];
static size_t   runtime_prefix_len;

/* libwebsockets state */
static struct lws_context      *lws_ctx;
static struct lws_vhost        *lws_vh;

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

static const char *
get_header(struct client *cl, const char *name)
{
    struct blob_attr *cur;
    int rem;

    /* uhttpd stores parsed request headers in cl->hdr as a blobmsg. Names
     * are lowercased on the way in; iterate case-insensitively so callers
     * can pass the canonical mixed-case form they wrote in the spec. */
    blob_for_each_attr(cur, cl->hdr.head, rem)
        if (!strcasecmp(blobmsg_name(cur), name))
            return blobmsg_get_string(cur);
    return NULL;
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
        if (!strncasecmp(p, WS_BEARER_TAG, tag_len)) {
            const char *sid = p + tag_len;
            size_t i;
            for (i = 0; i < 32; i++)
                if (!((sid[i] >= '0' && sid[i] <= '9') ||
                      (sid[i] >= 'a' && sid[i] <= 'f') ||
                      (sid[i] >= 'A' && sid[i] <= 'F')))
                    break;
            if (i == 32) return sid;
        }
        p = strchr(p, ',');
    }
    return NULL;
}

static bool
ws_authenticate(struct client *cl, char out_sid[33])
{
    const char *sid_src = NULL;
    const char *swp = get_header(cl, "Sec-WebSocket-Protocol");
    const char *auth = get_header(cl, "Authorization");
    struct blob_buf req = {};
    int err;

    if (swp) sid_src = find_sid_in_subprotocol(swp);

    if (!sid_src && auth && !strncasecmp(auth, "Bearer ", 7)) {
        const char *s = auth + 7;
        while (*s == ' ') s++;
        if (strlen(s) == 32) sid_src = s;
    }

    if (!sid_src) return false;

    memcpy(out_sid, sid_src, 32);
    out_sid[32] = '\0';

    blob_buf_init(&req, 0);
    blobmsg_add_string(&req, "ubus_rpc_session", out_sid);
    blobmsg_add_string(&req, "scope", "ubus");
    blobmsg_add_string(&req, "object", "session");
    blobmsg_add_string(&req, "function", "access");

    err = ubus_invoke(ubus_ctx, session_obj_id, "access",
                      req.head, NULL, NULL, 1000);
    blob_buf_free(&req);

    return (err == 0 || err == UBUS_STATUS_PERMISSION_DENIED);
}

static void
ws_acl_reply_cb(struct ubus_request *r, int type, struct blob_attr *msg)
{
    bool *allowed = r->priv;
    struct blob_attr *cur;
    int rem;

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

    blob_buf_init(&req, 0);
    blobmsg_add_string(&req, "ubus_rpc_session", sid);
    blobmsg_add_string(&req, "scope", "ubus");
    blobmsg_add_string(&req, "object", obj);
    blobmsg_add_string(&req, "function", fn);

    err = ubus_invoke(ubus_ctx, session_obj_id, "access",
                      req.head, ws_acl_reply_cb, &allowed, 1000);
    blob_buf_free(&req);

    return (err == 0) && allowed;
}

/* ---- Origin validation (CSWSH defense in depth) ----------------------- */

static const char *
hostport_from_origin(const char *origin, char *buf, size_t buflen)
{
    const char *p, *end;
    size_t n;

    p = strstr(origin, "://");
    if (!p) return NULL;
    p += 3;

    end = strchr(p, '/');
    n = end ? (size_t)(end - p) : strlen(p);
    if (n == 0 || n >= buflen) return NULL;

    memcpy(buf, p, n);
    buf[n] = '\0';
    return buf;
}

static bool
ws_origin_ok(struct client *cl)
{
    const char *origin = get_header(cl, "Origin");
    const char *host   = get_header(cl, "Host");
    char obuf[256];

    if (!origin) return true;                  /* non-browser -- allow */
    if (!host)   return false;

    if (!hostport_from_origin(origin, obuf, sizeof(obuf)))
        return false;

    return strcasecmp(obuf, host) == 0;
}

/* ---- request reconstruction for lws_adopt_socket_readbuf -------------- *
 *
 * uhttpd has already parsed the HTTP upgrade request: headers live in
 * cl->hdr (blobmsg, lowercased names), and the URL came in as the `url`
 * parameter to our handle_request callback. Rebuild the raw HTTP request
 * bytes so libwebsockets can drive its own handshake state machine.
 *
 * lws_adopt_socket_readbuf caps the readbuf at 2048 bytes (the ah rx buf);
 * a typical browser WS upgrade is 300-800 bytes so this fits comfortably.
 */
static int
ws_rebuild_request(struct client *cl, const char *url, char *buf, size_t buflen)
{
    int total, n, rem;
    struct blob_attr *cur;

    n = snprintf(buf, buflen, "GET %s HTTP/1.1\r\n", url ? url : "/");
    if (n < 0 || (size_t)n >= buflen) return -1;
    total = n;

    blob_for_each_attr(cur, cl->hdr.head, rem) {
        const char *hn = blobmsg_name(cur);
        const char *hv = blobmsg_get_string(cur);
        n = snprintf(buf + total, buflen - total, "%s: %s\r\n", hn, hv);
        if (n < 0 || (size_t)(total + n) >= buflen) return -1;
        total += n;
    }

    if ((size_t)(total + 2) >= buflen) return -1;
    memcpy(buf + total, "\r\n", 2);
    total += 2;

    return total;
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

static void
ws_call_reply_cb(struct ubus_request *r, int type, struct blob_attr *msg)
{
    struct json_object **out = r->priv;
    char *s = blobmsg_format_json(msg, true);
    *out = s ? json_tokener_parse(s) : NULL;
    free(s);
}

static void
ws_dispatch_call(struct ws_conn *c, struct json_object *id, struct json_object *params)
{
    const char *obj, *fn;
    struct json_object *jobj, *jfn, *jargs = NULL;
    struct blob_buf req = {};
    uint32_t oid;
    int err;
    struct json_object *out = NULL;
    struct json_object *result;

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

    if (!ws_acl_check(c->sid, obj, fn)) {
        ws_send_error(c, id, RPC_ERR_ACCESS, "Access denied"); return;
    }
    if (ubus_lookup_id(ubus_ctx, obj, &oid)) {
        ws_send_error(c, id, RPC_ERR_METHOD, "Object not found"); return;
    }

    blob_buf_init(&req, 0);
    if (jargs && json_object_is_type(jargs, json_type_object))
        blobmsg_add_object(&req, jargs);
    blobmsg_add_string(&req, "ubus_rpc_session", c->sid);

    err = ubus_invoke(ubus_ctx, oid, fn, req.head,
                      ws_call_reply_cb, &out, 30000);
    blob_buf_free(&req);

    if (err) {
        ws_send_error(c, id, -32000 - err, ubus_strerror(err));
        if (out) json_object_put(out);
        return;
    }

    result = json_object_new_array();
    json_object_array_add(result, json_object_new_int(0));
    if (out) json_object_array_add(result, out);
    ws_send_result(c, id, result);
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
    if (!ws_acl_check(c->sid, path, ":subscribe")) {
        ws_send_error(c, id, RPC_ERR_ACCESS, "Access denied"); return;
    }
    if (ubus_lookup_id(ubus_ctx, path, &oid)) {
        ws_send_error(c, id, RPC_ERR_METHOD, "Object not found"); return;
    }

    s = calloc(1, sizeof(*s) + strlen(path) + 1);
    if (!s) { ws_send_error(c, id, RPC_ERR_INTERNAL, "OOM"); return; }
    s->conn = c;
    s->obj_id = oid;
    strcpy(s->path, path);
    s->node.key = s->path;
    s->sub.cb = ws_notify_cb;

    err = ubus_register_subscriber(ubus_ctx, &s->sub);
    if (!err) err = ubus_subscribe(ubus_ctx, &s->sub, oid);
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

    req = json_tokener_parse_ex(tok, json, len);
    if (!req || !json_object_is_type(req, json_type_object)) {
        ws_send_error(c, NULL, RPC_ERR_PARSE, "Parse error"); rv = -1; goto out;
    }
    if (!json_object_object_get_ex(req, "jsonrpc", &jver) ||
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

    frame  = json_object_new_object();
    params = json_object_new_array();
    json_object_array_add(params, json_object_new_string(s->path));
    json_object_array_add(params, json_object_new_string(method ? method : ""));

    blob_json = msg ? blobmsg_format_json(msg, true) : NULL;
    data = blob_json ? json_tokener_parse(blob_json) : json_object_new_object();
    free(blob_json);
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

    case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
        /* Optional rejection point. We've already done auth + origin
         * pre-adopt, so allow here. */
        return 0;

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
    { NULL, NULL, 0, 0 }    /* terminator */
};

/* ---- uhttpd dispatch entry -------------------------------------------- */

static void
ws_handle_request(struct client *cl, char *url, struct path_info *pi)
{
    char  sid[33];
    char  reqbuf[2048];     /* lws ah rx buf limit */
    int   rlen, fd;
    char *pending_sid;
    struct lws *wsi;

    (void)pi;

    /* CSWSH defense */
    if (!ws_origin_ok(cl)) {
        const char *o = get_header(cl, "Origin");
        const char *h = get_header(cl, "Host");
        char detail[256];
        snprintf(detail, sizeof(detail),
                 "Origin '%s' does not match Host '%s'",
                 o ? o : "(missing)", h ? h : "(missing)");
        ops->client_error(cl, 403, "Forbidden", detail);
        return;
    }

    /* Bearer/SWP auth */
    if (!ws_authenticate(cl, sid)) {
        ops->client_error(cl, 401, "Unauthorized",
            "Missing or invalid Bearer token");
        return;
    }

    /* Rebuild the HTTP request bytes so libwebsockets can do the
     * upgrade handshake itself. */
    rlen = ws_rebuild_request(cl, url, reqbuf, sizeof(reqbuf));
    if (rlen < 0) {
        ops->client_error(cl, 400, "Bad Request", "Upgrade request too large");
        return;
    }

    /* dup the socket fd so libwebsockets gets an independent reference.
     * uhttpd will close its original fd via client_close below; the
     * kernel TCP connection survives because lws holds the dup. */
    fd = dup(cl->sfd.fd);
    if (fd < 0) {
        ops->client_error(cl, 500, "Internal Error", "dup() failed");
        return;
    }

    /* Stash the sid for the ESTABLISHED callback to pick up. Heap-allocated
     * because lws callbacks run later in uloop; freed in ESTABLISHED or
     * WSI_DESTROY (covers the disconnect-before-established race). */
    pending_sid = malloc(33);
    if (!pending_sid) {
        close(fd);
        ops->client_error(cl, 500, "Internal Error", "OOM");
        return;
    }
    memcpy(pending_sid, sid, 33);

    wsi = lws_adopt_socket_readbuf(lws_ctx, fd, reqbuf, (size_t)rlen);
    if (!wsi) {
        /* lws_adopt_socket_readbuf already closed the fd on failure */
        free(pending_sid);
        ops->client_error(cl, 500, "Internal Error", "lws_adopt failed");
        return;
    }
    lws_set_opaque_user_data(wsi, pending_sid);

    /* Tell uhttpd we're done with this client. uhttpd closes its fd; lws
     * keeps the dup. The dispatch handler returns and uhttpd reclaims cl. */
    ops->client_close(cl);
}

/* ---- plugin registration ---------------------------------------------- */

static bool
ws_check_url(const char *url)
{
    if (runtime_prefix_len == 0) return false;
    return strncmp(url, runtime_prefix, runtime_prefix_len) == 0 &&
           (url[runtime_prefix_len] == '\0' ||
            url[runtime_prefix_len] == '?' ||
            url[runtime_prefix_len] == '/');
}

static struct dispatch_handler ws_dispatch = {
    .script = false,
    .check_url = ws_check_url,
    .handle_request = ws_handle_request,
};

static int
ws_lws_init(void)
{
    struct lws_context_creation_info info = { 0 };

    info.port              = CONTEXT_PORT_NO_LISTEN;   /* we don't listen; uhttpd does */
    info.protocols         = ws_protocols;
    info.gid               = -1;
    info.uid               = -1;
    info.options           = LWS_SERVER_OPTION_VALIDATE_UTF8 |
                             LWS_SERVER_OPTION_DISABLE_IPV6_LISTEN;
    /*
     * TODO(verify on-device): the OpenWrt libwebsockets build is compiled
     * with LWS_WITH_ULOOP=ON. The expected pattern for sharing uhttpd's
     * uloop is one of:
     *   (a) info.options |= LWS_SERVER_OPTION_ULOOP;
     *       info.foreign_loops = (void *[]){ NULL };   // use default uloop
     *   (b) Auto-detected -- lws picks up the running uloop when LWS_WITH_ULOOP
     *       was set at compile time.
     * Confirm by reading the lws context's selected event lib on first
     * connection. Adjust the flags if uloop integration is not active.
     */

    lws_ctx = lws_create_context(&info);
    if (!lws_ctx) return -1;

    /* Default vhost for adoption -- no listener, no certs (uhttpd holds them) */
    lws_vh = lws_create_vhost(lws_ctx, &info);
    if (!lws_vh) {
        lws_context_destroy(lws_ctx);
        lws_ctx = NULL;
        return -1;
    }
    return 0;
}

static int
ws_plugin_init(const struct uhttpd_ops *o, struct config *c)
{
    int n;

    ops = o;
    _conf = c;

    if (!c->ubus_prefix || !*c->ubus_prefix) {
        fprintf(stderr, "uhttpd-mod-ws: ubus_prefix not set; not registered\n");
        return 0;
    }

    n = snprintf(runtime_prefix, sizeof(runtime_prefix), "%s", c->ubus_prefix);
    if (n < 0 || (size_t)n >= sizeof(runtime_prefix)) {
        fprintf(stderr, "uhttpd-mod-ws: ubus_prefix too long\n");
        return -1;
    }
    while (n > 0 && runtime_prefix[n - 1] == '/')
        runtime_prefix[--n] = '\0';

    if ((size_t)n + sizeof(WS_SUFFIX) > sizeof(runtime_prefix)) {
        fprintf(stderr, "uhttpd-mod-ws: derived prefix too long\n");
        return -1;
    }
    memcpy(runtime_prefix + n, WS_SUFFIX, sizeof(WS_SUFFIX));
    runtime_prefix_len = n + sizeof(WS_SUFFIX) - 1;

    ubus_ctx = ubus_connect(c->ubus_socket);
    if (!ubus_ctx) {
        fprintf(stderr, "uhttpd-mod-ws: ubus_connect(%s) failed\n",
                c->ubus_socket ? c->ubus_socket : "default");
        return -1;
    }
    ubus_add_uloop(ubus_ctx);

    if (ubus_lookup_id(ubus_ctx, "session", &session_obj_id)) {
        fprintf(stderr, "uhttpd-mod-ws: rpcd 'session' not found "
                        "(install rpcd?)\n");
    }

    if (ws_lws_init() < 0) {
        fprintf(stderr, "uhttpd-mod-ws: libwebsockets init failed\n");
        return -1;
    }

    ops->dispatch_add(&ws_dispatch);
    fprintf(stderr,
        "uhttpd-mod-ws: serving WebSocket JSON-RPC at %s (lws-backed)\n",
        runtime_prefix);
    return 0;
}

struct uhttpd_plugin uhttpd_plugin = {
    .init = ws_plugin_init,
};
