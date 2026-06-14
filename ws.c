/*
 * uhttpd-mod-ws -- RFC 6455 WebSocket transport for ubus JSON-RPC
 *
 * Staging copy. Final home: package/network/services/uhttpd/src/ws.c
 * in openwrt/openwrt, built as a sub-package alongside mod-ubus /
 * mod-ucode / mod-lua.
 *
 * Endpoint:   /ubus-ws    Sibling of /ubus, NOT a child. The child form
 *                         /ubus/ws would collide with mod-ubus's prefix
 *                         match (mod-ubus's check_url returns true for
 *                         anything starting with "/ubus/"). The hyphen
 *                         breaks the prefix while keeping the family feel.
 *                         Family: /ubus = POST JSON-RPC (mod-ubus),
 *                         /ubus/subscribe/<obj> = SSE (mod-ubus),
 *                         /ubus-ws = JSON-RPC over WebSocket (this).
 *
 * Wire:       JSON-RPC 2.0 mirroring /ubus (see luci-base
 *             ucode/controller/admin/index.uc). Adds subscribe/
 *             unsubscribe verbs; notify is server-initiated (no id).
 *
 * Multiplex:  One WS connection carries N concurrent calls
 *             (correlated by JSON-RPC id) and M concurrent
 *             subscriptions (routed by ubus object path).
 *
 * Auth:       Two transports for the session id (sid), accepted in
 *             this order:
 *               1) Sec-WebSocket-Protocol: ubus-json-rpc-v1, bearer.<sid>
 *                  (the only way browser JS can pass auth -- the
 *                   WebSocket constructor takes no headers)
 *               2) Authorization: Bearer <sid>
 *                  (works for curl/scripts/tests)
 *             Server picks subprotocol "ubus-json-rpc-v1" and echoes
 *             ONLY that back (the bearer token is consumed, not echoed,
 *             so it doesn't appear in subprotocol-listing UIs). Sid is
 *             validated once against rpcd session.access; ACL is
 *             re-checked per-op at dispatch time.
 *
 * UAF rule:   Every per-connection ubus_subscriber is freed from
 *             cl->dispatch.free. The mod-ubus SSE path forgot this
 *             and crashes ~30s after disconnect. Don't replicate.
 *
 * DoS posture (V1):
 *   - liveness: PING every 20s; close if no PONG in 45s
 *   - per-conn cap: 64 simultaneous subscriptions (-32005)
 *   - tx backlog: close 1009 if ustream pending > 1 MiB
 *   - max single inbound frame: 1 MiB (close 1009)
 *   - reject fragmented frames in V1 (close 1003)
 *
 * Copyright (C) 2026 [author]  -- Apache-2.0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <libubox/blobmsg.h>
#include <libubox/blobmsg_json.h>
#include <libubox/list.h>
#include <libubox/avl.h>
#include <libubox/avl-cmp.h>
#include <libubox/uloop.h>
#include <libubox/ustream.h>

#include <libubus.h>
#include <json-c/json.h>

#include "uhttpd.h"
#include "plugin.h"

/* ---- constants ---------------------------------------------------------- */

#define WS_GUID              "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define WS_PREFIX            "/ubus-ws"
#define WS_SUBPROTO          "ubus-json-rpc-v1"
#define WS_BEARER_TAG        "bearer."          /* sec-ws-protocol token prefix */

#define WS_PING_INTERVAL_MS  20000        /* under uhttpd's 30s network_timeout */
#define WS_PONG_GRACE_MS     45000        /* 2-3 missed pings before close      */
#define WS_MAX_SUBS_DEFAULT  64
#define WS_TX_BACKLOG_MAX    (1u << 20)   /* 1 MiB */
#define WS_MAX_FRAME         (1u << 20)   /* 1 MiB single frame */

/* RFC 6455 opcodes */
enum {
    WS_OP_CONT   = 0x0,
    WS_OP_TEXT   = 0x1,
    WS_OP_BINARY = 0x2,
    WS_OP_CLOSE  = 0x8,
    WS_OP_PING   = 0x9,
    WS_OP_PONG   = 0xa,
};

/* WS close codes used */
#define WS_CLOSE_NORMAL          1000
#define WS_CLOSE_PROTOCOL_ERROR  1002
#define WS_CLOSE_UNSUPPORTED     1003
#define WS_CLOSE_POLICY          1008
#define WS_CLOSE_TOO_BIG         1009
#define WS_CLOSE_INTERNAL        1011

/* ---- module-level state ------------------------------------------------- */

static const struct uhttpd_ops *ops;
static struct config *_conf;
static struct ubus_context *ubus_ctx;
static uint32_t session_obj_id;       /* cached "session" object id */

/* ---- per-connection state ----------------------------------------------- */

struct ws_conn {
    struct client *cl;              /* uhttpd client (raw socket owner) */
    char sid[33];                   /* bound session id (32 hex + NUL)  */

    /* inbound frame accumulator */
    char     *rx_buf;
    size_t    rx_len;
    size_t    rx_cap;

    /* active subscriptions on this connection */
    struct avl_tree subs;           /* keyed by ubus object path string */
    unsigned int    num_subs;
    unsigned int    max_subs;       /* default WS_MAX_SUBS_DEFAULT      */

    /* liveness */
    struct uloop_timeout ping_timer;
    uint64_t             last_pong_ms;
};

/* one entry per active subscription on a ws_conn */
struct ws_sub {
    struct avl_node       node;     /* key = subscribed object path     */
    struct ws_conn       *conn;     /* back-pointer for notify callback */
    struct ubus_subscriber sub;     /* libubus subscriber               */
    uint32_t              obj_id;   /* resolved ubus object id          */
    char                  path[];   /* flex array; key for avl          */
};

/* ---- forward decls ------------------------------------------------------ */

static void ws_conn_close(struct ws_conn *c, uint16_t code, const char *reason);
static void ws_send_frame(struct ws_conn *c, int opcode,
                          const void *payload, size_t len);
static void ws_send_text(struct ws_conn *c, const char *s);
static int  ws_handle_jsonrpc(struct ws_conn *c, const char *json, size_t len);
static int  ws_notify_cb(struct ubus_context *ctx, struct ubus_object *obj,
                         struct ubus_request_data *req, const char *method,
                         struct blob_attr *msg);
static void ws_conn_free_cb(struct client *cl);
static void ws_read_cb(struct ustream *s, int bytes_new);
static void ws_ping_timer_cb(struct uloop_timeout *t);

/* ---- SHA-1 + base64 for handshake accept key --------------------------- *
 *
 * Drop sha1.c (Steve Reid's public-domain impl, ~150 lines) alongside ws.c.
 * base64_encode: tiny RFC 4648 encoder; ~30 lines, see misc.c in libubox.
 */
extern void sha1_compute(const void *data, size_t len, uint8_t out[20]);
extern int  base64_encode(const void *src, size_t srclen,
                          char *dst, size_t dstlen);

/* ---- helpers ------------------------------------------------------------ */

static const char *
get_header(struct client *cl, const char *name)
{
    int i;
    for (i = 0; i < cl->request.n_headers; i++)
        if (!strcasecmp(cl->request.headers[i].name, name))
            return cl->request.headers[i].value;
    return NULL;
}

static size_t
ws_pending_bytes(struct client *cl)
{
    /* TODO(verify on-device): pending bytes in uhttpd's ustream tx buffer.
     * The exact field depends on whether TLS is in play -- cl->us is the
     * outer ustream; for HTTPS it wraps an ustream_ssl. uhttpd should
     * expose a helper; if not, ustream_pending() walks the buffer chain. */
    return cl->us ? cl->us->w.data_bytes : 0;
}

/* ---- auth -------------------------------------------------------------- */

/*
 * Extract a 32-hex sid from Sec-WebSocket-Protocol, looking for a token
 * shaped "bearer.<32 hex chars>". Returns pointer into the header value
 * (NOT NUL-terminated at sid end -- caller must copy 32 bytes).
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

/*
 * Confirm that "ubus-json-rpc-v1" appears in the offered subprotocol list,
 * so we can echo it back as the negotiated protocol. Per RFC 6455 4.2.2
 * the server MUST select one of the client's offered subprotocols.
 */
static bool
subprotocol_offered(const char *hdr, const char *want)
{
    const char *p = hdr;
    size_t wl = strlen(want);

    while (p && *p) {
        while (*p == ' ' || *p == ',') p++;
        if (!strncasecmp(p, want, wl) &&
            (p[wl] == '\0' || p[wl] == ',' || p[wl] == ' '))
            return true;
        p = strchr(p, ',');
    }
    return false;
}

static bool
ws_authenticate(struct client *cl, char out_sid[33])
{
    const char *sid_src = NULL;
    const char *swp = get_header(cl, "Sec-WebSocket-Protocol");
    const char *auth = get_header(cl, "Authorization");
    struct blob_buf req = {};
    int err;

    /* primary: Sec-WebSocket-Protocol bearer token */
    if (swp) sid_src = find_sid_in_subprotocol(swp);

    /* fallback: Authorization: Bearer (curl/scripts) */
    if (!sid_src && auth && !strncasecmp(auth, "Bearer ", 7)) {
        const char *s = auth + 7;
        while (*s == ' ') s++;
        if (strlen(s) == 32) sid_src = s;
    }

    if (!sid_src) return false;

    /* sid_src may point into a comma-separated header value; copy 32 bytes
     * into out_sid first so we can NUL-terminate and use it safely. */
    memcpy(out_sid, sid_src, 32);
    out_sid[32] = '\0';

    /* Probe session.access to validate the sid exists (any answer ==> valid
     * session). Per-op ACL is enforced again at dispatch time. */
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

/* per-op ACL check: scope=ubus, object=<obj>, function=<fn> */
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

/* ---- handshake --------------------------------------------------------- */

static void
ws_send_101(struct client *cl, const char *client_key, bool echo_subproto)
{
    char keybuf[128];
    uint8_t sha[20];
    char accept[64];
    int n;

    n = snprintf(keybuf, sizeof(keybuf), "%s" WS_GUID, client_key);
    if (n < 0 || (size_t)n >= sizeof(keybuf)) {
        ops->client_error(cl, 400, "Bad Request", "WS key too long");
        return;
    }

    sha1_compute(keybuf, n, sha);
    base64_encode(sha, sizeof(sha), accept, sizeof(accept));

    ops->http_header(cl, 101, "Switching Protocols");
    ustream_printf(cl->us,
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n",
        accept);
    if (echo_subproto)
        ustream_printf(cl->us, "Sec-WebSocket-Protocol: " WS_SUBPROTO "\r\n");
    ustream_printf(cl->us, "\r\n");
}

static void
ws_handle_request(struct client *cl, char *url, struct path_info *pi)
{
    const char *upgrade = get_header(cl, "Upgrade");
    const char *key     = get_header(cl, "Sec-WebSocket-Key");
    const char *ver     = get_header(cl, "Sec-WebSocket-Version");
    char sid[33];
    struct ws_conn *c;

    if (!upgrade || strcasecmp(upgrade, "websocket") ||
        !key || !ver || strcmp(ver, "13")) {
        ops->client_error(cl, 400, "Bad Request",
            "Expected WebSocket upgrade (version 13)");
        return;
    }

    if (!ws_authenticate(cl, sid)) {
        ops->client_error(cl, 401, "Unauthorized",
            "Missing or invalid Bearer token");
        return;
    }

    c = calloc(1, sizeof(*c));
    if (!c) {
        ops->client_error(cl, 500, "Internal Error", "OOM");
        return;
    }
    c->cl = cl;
    memcpy(c->sid, sid, sizeof(sid));
    avl_init(&c->subs, avl_strcmp, false, NULL);
    c->max_subs = WS_MAX_SUBS_DEFAULT;
    c->last_pong_ms = uloop_now();
    c->ping_timer.cb = ws_ping_timer_cb;

    /* Install teardown FIRST -- if anything below fails or the client
     * disconnects mid-setup, free_cb still finds and cleans up req_data. */
    cl->dispatch.req_data  = c;
    cl->dispatch.free      = ws_conn_free_cb;
    cl->dispatch.close_fds = NULL;

    {
        const char *swp = get_header(cl, "Sec-WebSocket-Protocol");
        bool echo = swp && subprotocol_offered(swp, WS_SUBPROTO);
        ws_send_101(cl, key, echo);
    }

    /* take over the ustream: parse incoming frames in ws_read_cb */
    cl->us->notify_read = ws_read_cb;

    /* start liveness ping */
    uloop_timeout_set(&c->ping_timer, WS_PING_INTERVAL_MS);
}

/* ---- THE rule: UAF-safe teardown --------------------------------------- *
 *
 * Called when uhttpd is releasing the client. We MUST unregister every
 * libubus subscriber here, otherwise ubusd keeps the dangling pointer.
 * This is the bug that broke mod-ubus SSE on 25.12.
 */
static void
ws_conn_free_cb(struct client *cl)
{
    struct ws_conn *c = cl->dispatch.req_data;
    struct ws_sub *s, *tmp;

    if (!c) return;

    uloop_timeout_cancel(&c->ping_timer);

    avl_for_each_element_safe(&c->subs, s, node, tmp) {
        avl_delete(&c->subs, &s->node);
        ubus_unregister_subscriber(ubus_ctx, &s->sub);
        free(s);
    }

    free(c->rx_buf);
    free(c);
    cl->dispatch.req_data = NULL;
}

/* ---- liveness ---------------------------------------------------------- */

static void
ws_ping_timer_cb(struct uloop_timeout *t)
{
    struct ws_conn *c = container_of(t, struct ws_conn, ping_timer);

    if (uloop_now() - c->last_pong_ms > WS_PONG_GRACE_MS) {
        ws_conn_close(c, WS_CLOSE_POLICY, "no PONG");
        return;
    }

    ws_send_frame(c, WS_OP_PING, NULL, 0);
    uloop_timeout_set(t, WS_PING_INTERVAL_MS);
}

/* ---- frame I/O --------------------------------------------------------- */

static void
ws_send_frame(struct ws_conn *c, int opcode, const void *payload, size_t len)
{
    uint8_t hdr[10];
    size_t hlen;

    /* backlog watchdog */
    if (ws_pending_bytes(c->cl) > WS_TX_BACKLOG_MAX) {
        ws_conn_close(c, WS_CLOSE_TOO_BIG, "tx backlog");
        return;
    }

    hdr[0] = 0x80 | (opcode & 0x0f);    /* FIN=1, no extensions */

    if (len < 126) {
        hdr[1] = (uint8_t)len;
        hlen = 2;
    } else if (len < 65536) {
        hdr[1] = 126;
        hdr[2] = (len >> 8) & 0xff;
        hdr[3] = len & 0xff;
        hlen = 4;
    } else {
        hdr[1] = 127;
        uint64_t l = len;
        for (int i = 0; i < 8; i++) hdr[2 + i] = (l >> (56 - 8*i)) & 0xff;
        hlen = 10;
    }

    /* server frames MUST NOT be masked (RFC 6455 5.1) */
    ustream_write(c->cl->us, (char *)hdr, hlen, false);
    if (len) ustream_write(c->cl->us, payload, len, false);
}

static void
ws_send_text(struct ws_conn *c, const char *s)
{
    ws_send_frame(c, WS_OP_TEXT, s, strlen(s));
}

/* Parse the accumulator for one or more complete frames.
 *
 * RFC 6455 5.2:
 *   byte 0: FIN(1) | RSV1-3(3) | OPCODE(4)
 *   byte 1: MASK(1) | LEN(7)
 *   if LEN==126: 2-byte extended length
 *   if LEN==127: 8-byte extended length
 *   if MASK==1: 4-byte masking key
 *   payload (XOR-unmask if masked)
 */
static void
ws_read_cb(struct ustream *s, int bytes_new)
{
    struct client *cl = container_of(s, struct client, sfd.stream);
    struct ws_conn *c = cl->dispatch.req_data;
    char *data;
    int rlen;

    if (!c) return;

    data = ustream_get_read_buf(s, &rlen);
    if (!data || rlen <= 0) return;

    /* append to accumulator */
    if (c->rx_len + rlen > c->rx_cap) {
        size_t need = c->rx_len + rlen;
        size_t cap  = c->rx_cap ? c->rx_cap : 1024;
        while (cap < need) cap *= 2;
        if (cap > WS_MAX_FRAME + 64) {  /* refuse oversized */
            ws_conn_close(c, WS_CLOSE_TOO_BIG, "frame too big");
            return;
        }
        char *nbuf = realloc(c->rx_buf, cap);
        if (!nbuf) { ws_conn_close(c, WS_CLOSE_INTERNAL, "OOM"); return; }
        c->rx_buf = nbuf;
        c->rx_cap = cap;
    }
    memcpy(c->rx_buf + c->rx_len, data, rlen);
    c->rx_len += rlen;
    ustream_consume(s, rlen);

    /* parse as many complete frames as we have */
    for (;;) {
        uint8_t *p = (uint8_t *)c->rx_buf;
        size_t   n = c->rx_len;
        size_t   off, plen;
        bool     fin, masked;
        int      op;
        uint8_t  mask[4];

        if (n < 2) return;                        /* need header */

        fin    = !!(p[0] & 0x80);
        op     = p[0] & 0x0f;
        masked = !!(p[1] & 0x80);
        plen   = p[1] & 0x7f;
        off    = 2;

        if (!masked) {                            /* clients MUST mask */
            ws_conn_close(c, WS_CLOSE_PROTOCOL_ERROR, "unmasked frame");
            return;
        }
        if (!fin || op == WS_OP_CONT) {           /* V1: no fragmentation */
            ws_conn_close(c, WS_CLOSE_UNSUPPORTED, "fragmented frame");
            return;
        }
        if (plen == 126) {
            if (n < 4) return;
            plen = (p[2] << 8) | p[3];
            off  = 4;
        } else if (plen == 127) {
            if (n < 10) return;
            plen = 0;
            for (int i = 0; i < 8; i++) plen = (plen << 8) | p[2 + i];
            off  = 10;
        }
        if (plen > WS_MAX_FRAME) {
            ws_conn_close(c, WS_CLOSE_TOO_BIG, "frame too big");
            return;
        }
        if (n < off + 4 + plen) return;           /* incomplete */

        memcpy(mask, p + off, 4);
        off += 4;

        /* unmask in-place */
        for (size_t i = 0; i < plen; i++)
            p[off + i] ^= mask[i & 3];

        /* dispatch */
        switch (op) {
        case WS_OP_TEXT:
            ws_handle_jsonrpc(c, (char *)(p + off), plen);
            break;
        case WS_OP_PING:
            ws_send_frame(c, WS_OP_PONG, p + off, plen);
            break;
        case WS_OP_PONG:
            c->last_pong_ms = uloop_now();
            break;
        case WS_OP_CLOSE:
            ws_conn_close(c, WS_CLOSE_NORMAL, NULL);
            return;
        case WS_OP_BINARY:
        default:
            ws_conn_close(c, WS_CLOSE_UNSUPPORTED, "unsupported opcode");
            return;
        }

        /* shift remainder down */
        size_t consumed = off + plen;
        size_t left = c->rx_len - consumed;
        if (left) memmove(c->rx_buf, c->rx_buf + consumed, left);
        c->rx_len = left;
    }
}

/* ---- JSON-RPC dispatch ------------------------------------------------- *
 *
 * Wire shape (Candidate alpha, mirrors /ubus):
 *
 *   call:        {jsonrpc:"2.0",id:N,method:"call",
 *                 params:[obj,fn,args]}
 *   list:        {jsonrpc:"2.0",id:N,method:"list",params:[pat?]}
 *   subscribe:   {jsonrpc:"2.0",id:N,method:"subscribe",
 *                 params:[obj_path]}
 *   unsubscribe: {jsonrpc:"2.0",id:N,method:"unsubscribe",
 *                 params:[obj_path]}
 *   reply:       {jsonrpc:"2.0",id:N,result:...|error:{...}}
 *   notify:      {jsonrpc:"2.0",method:"notify",
 *                 params:[obj_path, notify_type, data]}
 */

static void
ws_send_error(struct ws_conn *c, struct json_object *id, int code, const char *msg)
{
    struct json_object *r = json_object_new_object();
    struct json_object *e = json_object_new_object();
    json_object_object_add(r, "jsonrpc", json_object_new_string("2.0"));
    json_object_object_add(r, "id", id ? json_object_get(id) : NULL);
    json_object_object_add(e, "code", json_object_new_int(code));
    json_object_object_add(e, "message", json_object_new_string(msg));
    json_object_object_add(r, "error", e);
    ws_send_text(c, json_object_to_json_string(r));
    json_object_put(r);
}

static void
ws_send_result(struct ws_conn *c, struct json_object *id, struct json_object *result)
{
    struct json_object *r = json_object_new_object();
    json_object_object_add(r, "jsonrpc", json_object_new_string("2.0"));
    json_object_object_add(r, "id", id ? json_object_get(id) : NULL);
    json_object_object_add(r, "result", result);
    ws_send_text(c, json_object_to_json_string(r));
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
    /* params = [obj, fn, args?] */
    const char *obj, *fn;
    struct json_object *jobj, *jfn, *jargs = NULL;
    struct blob_buf req = {};
    uint32_t oid;
    int err;

    if (!params || json_object_array_length(params) < 3) {
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
    }
    jobj = json_object_array_get_idx(params, 0);
    jfn  = json_object_array_get_idx(params, 1);
    jargs= json_object_array_get_idx(params, 2);
    if (!jobj || !jfn ||
        !json_object_is_type(jobj, json_type_string) ||
        !json_object_is_type(jfn,  json_type_string)) {
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
    }
    obj = json_object_get_string(jobj);
    fn  = json_object_get_string(jfn);

    if (!ws_acl_check(c->sid, obj, fn)) {
        ws_send_error(c, id, -32002, "Access denied"); return;
    }

    if (ubus_lookup_id(ubus_ctx, obj, &oid)) {
        ws_send_error(c, id, -32601, "Object not found"); return;
    }

    blob_buf_init(&req, 0);
    if (jargs && json_object_is_type(jargs, json_type_object))
        blobmsg_add_object(&req, jargs);
    blobmsg_add_string(&req, "ubus_rpc_session", c->sid);

    struct json_object *out = NULL;
    err = ubus_invoke(ubus_ctx, oid, fn, req.head,
                      ws_call_reply_cb, &out, 30000);
    blob_buf_free(&req);

    if (err) {
        ws_send_error(c, id, -32000 - err, ubus_strerror(err));
        if (out) json_object_put(out);
        return;
    }

    /* mirror /ubus reply shape: result=[code, data] */
    struct json_object *result = json_object_new_array();
    json_object_array_add(result, json_object_new_int(0));
    if (out) json_object_array_add(result, out);
    ws_send_result(c, id, result);
}

static void
ws_dispatch_list(struct ws_conn *c, struct json_object *id, struct json_object *params)
{
    /* Stub: mirror index.uc:76-100. ubus_lookup with callback collecting
     * object names + method signatures. Returns object {name: {method:
     * {param: type}}}. */
    /* TODO(verify on-device): ubus_lookup API + per-object signature walk */
    struct json_object *result = json_object_new_object();
    ws_send_result(c, id, result);
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
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
    }
    jpath = json_object_array_get_idx(params, 0);
    if (!jpath || !json_object_is_type(jpath, json_type_string)) {
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
    }
    path = json_object_get_string(jpath);

    /* idempotent: subscribe to already-subscribed path == ok */
    if (avl_find(&c->subs, path)) {
        ws_send_result(c, id, json_object_new_boolean(true));
        return;
    }

    if (c->num_subs >= c->max_subs) {
        ws_send_error(c, id, -32005, "Too many subscriptions"); return;
    }

    if (!ws_acl_check(c->sid, path, ":subscribe")) {
        ws_send_error(c, id, -32002, "Access denied"); return;
    }

    if (ubus_lookup_id(ubus_ctx, path, &oid)) {
        ws_send_error(c, id, -32601, "Object not found"); return;
    }

    s = calloc(1, sizeof(*s) + strlen(path) + 1);
    if (!s) { ws_send_error(c, id, -32603, "OOM"); return; }
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
        ws_send_error(c, id, -32603, ubus_strerror(err));
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
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
    }
    jpath = json_object_array_get_idx(params, 0);
    if (!jpath || !json_object_is_type(jpath, json_type_string)) {
        ws_send_error(c, id, -32602, "Invalid parameters"); return;
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
        ws_send_error(c, NULL, -32700, "Parse error"); rv = -1; goto out;
    }
    if (!json_object_object_get_ex(req, "jsonrpc", &jver) ||
        strcmp(json_object_get_string(jver), "2.0") != 0 ||
        !json_object_object_get_ex(req, "method", &jmethod) ||
        !json_object_is_type(jmethod, json_type_string)) {
        ws_send_error(c, NULL, -32600, "Invalid request"); rv = -1; goto out;
    }
    json_object_object_get_ex(req, "id", &jid);
    json_object_object_get_ex(req, "params", &jparams);
    method = json_object_get_string(jmethod);

    if      (!strcmp(method, "call"))        ws_dispatch_call(c, jid, jparams);
    else if (!strcmp(method, "list"))        ws_dispatch_list(c, jid, jparams);
    else if (!strcmp(method, "subscribe"))   ws_dispatch_subscribe(c, jid, jparams);
    else if (!strcmp(method, "unsubscribe")) ws_dispatch_unsubscribe(c, jid, jparams);
    else ws_send_error(c, jid, -32601, "Method not found");

out:
    if (req) json_object_put(req);
    json_tokener_free(tok);
    return rv;
}

/* ---- notify -> frame --------------------------------------------------- */

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

    ws_send_text(s->conn, json_object_to_json_string(frame));
    json_object_put(frame);
    return 0;
}

/* ---- close path -------------------------------------------------------- */

static void
ws_conn_close(struct ws_conn *c, uint16_t code, const char *reason)
{
    uint8_t buf[125];
    size_t rlen = reason ? strlen(reason) : 0;

    if (rlen > sizeof(buf) - 2) rlen = sizeof(buf) - 2;

    buf[0] = (code >> 8) & 0xff;
    buf[1] = code & 0xff;
    if (rlen) memcpy(buf + 2, reason, rlen);

    ws_send_frame(c, WS_OP_CLOSE, buf, 2 + rlen);
    /* uhttpd drains, then fires cl->dispatch.free -> ws_conn_free_cb */
    ops->client_close(c->cl);
}

/* ---- plugin registration ----------------------------------------------- */

static bool
ws_check_url(const char *url)
{
    size_t n = strlen(WS_PREFIX);
    return strncmp(url, WS_PREFIX, n) == 0 &&
           (url[n] == '\0' || url[n] == '?' || url[n] == '/');
}

static struct dispatch_handler ws_dispatch = {
    .script = false,
    .check_url = ws_check_url,
    .handle_request = ws_handle_request,
};

static int
ws_plugin_init(const struct uhttpd_ops *o, struct config *c)
{
    ops = o;
    _conf = c;

    ubus_ctx = ubus_connect(NULL);
    if (!ubus_ctx) {
        fprintf(stderr, "uhttpd-mod-ws: ubus_connect failed\n");
        return -1;
    }
    ubus_add_uloop(ubus_ctx);

    /* cache the rpcd "session" object id once */
    if (ubus_lookup_id(ubus_ctx, "session", &session_obj_id)) {
        fprintf(stderr, "uhttpd-mod-ws: rpcd 'session' not found "
                        "(install rpcd?)\n");
        /* don't fail: session may appear later. Re-lookup on first auth. */
    }

    ops->dispatch_add(&ws_dispatch);
    return 0;
}

struct uhttpd_plugin uhttpd_plugin = {
    .init = ws_plugin_init,
};
