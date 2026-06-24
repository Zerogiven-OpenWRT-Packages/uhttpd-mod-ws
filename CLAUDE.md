# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## What this project is

`ubus-wsd` is a **standalone daemon** that exposes ubus JSON-RPC over a WebSocket endpoint. It listens on its own TLS port (default 8443), authenticates against rpcd's session.access just like uhttpd-mod-ubus does, and dispatches `call`, `list`, `subscribe`, and `unsubscribe` verbs to ubus.

It is NOT a uhttpd plugin. The earlier version of this project tried to be one (`ubus-wsd.so`), but uhttpd's plugin loader has a **hardcoded list** of plugins it will load (mod-ubus / mod-lua / mod-ucode) — there is no mechanism for third-party plugins to register. That architectural dead-end is why we became a standalone daemon. (Local-patched uhttpd was an option, but would have meant every consumer of our package needed to also ship a forked uhttpd build.)

## Where it fits

```
  Browser ──wss──▶ ubus-wsd ──libubus──▶ ubusd ◀── rpcd (session.access)
                                            ▲
                                            │
                                  uhttpd, mod-ubus, ucode etc.
                                  (untouched -- separate process)
```

- Endpoint: `wss://<host>:<port>/ubus-ws` where `<port>` is from `/etc/config/ubus-ws` (default 8443).
- Companion JS client: `files/www/luci-static/resources/rpc-ws.js` — installed to `/www/luci-static/resources/` on the device. LuCI views consume it with `'require rpc-ws';`. The client discovers the WS URL at runtime by calling `uci.load('ubus-ws')` over the standard LuCI/uhttpd HTTPS path; the resolved URL is cached for the page lifetime.
- ACL grant: `files/usr/share/rpcd/acl.d/ubus-wsd.json` grants `read uci.ubus-ws` so the JS client's `uci.load` succeeds for normal logged-in users.

## Required dependencies (runtime)

- `rpcd` (provides `session.access` for auth + ACL gating)
- `libubus` (RPC to ubusd)
- `libubox` (uloop, blobmsg, avl, list)
- `libblobmsg-json`, `libjson-c` (JSON ↔ blob translation)
- `libwebsockets` (RFC 6455 framing, TLS termination, the listening socket)
- `libuci` (read `/etc/config/ubus-ws` at startup)

## Build context

Standalone OpenWrt feed package. Top-level `Makefile` is the OpenWrt package descriptor; `src/Makefile` is the inner build that produces the `ubus-wsd` binary.

```sh
# Inside an OpenWrt SDK / buildroot with this repo wired as a feed:
./scripts/feeds update mymod && ./scripts/feeds install ubus-wsd
make menuconfig                # select ubus-wsd with 'm'
make package/ubus-wsd/compile V=s
# IPK lands in bin/packages/<arch>/mymod/
```

**No uhttpd source dependency.** Compared to the old plugin version, this build:
- Has no `PKG_BUILD_DEPENDS:=uhttpd`
- Doesn't need bundled uhttpd headers (the three `.h` files we used to vendor are gone)
- Doesn't define `HAVE_TLS` or any uhttpd-struct-layout flag
- Produces an executable, not a `.so`

## Critical design constraints

These are not optional. Honor them when modifying code.

### 1. UAF rule — every `ubus_subscriber` torn down from `LWS_CALLBACK_CLOSED`

Every per-connection `ubus_subscriber` registered with ubusd must be unregistered from `LWS_CALLBACK_CLOSED` via `ws_conn_teardown()`. If you only clean up from the ubusd-side `obj.remove_cb`, the subscriber outlives the client and ubusd's reaper dereferences a freed pointer ~30s later (silent crash, procd respawn).

This is the bug shipped in `uhttpd-mod-ubus`'s SSE path on 25.12. We don't repeat it.

### 2. Auth — two transports, sid bound at upgrade

- Primary: `Sec-WebSocket-Protocol: ubus-json-rpc-v1, bearer.<sid>` (the only way browser `WebSocket` constructors can pass auth — no header parameter).
- Fallback: `Authorization: Bearer <sid>` (curl/scripts/tests).
- Sid is validated once against `rpcd session.access` in `LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION` (BEFORE the handshake response). Per-op ACL is re-checked at dispatch time. Sid binding lives in `struct ws_conn::sid[33]` and is never reset for the life of the connection.

### 2a. Origin — CSWSH defense in depth

The upgrade handshake also validates the `Origin:` header:
- Default policy: same-hostname, ports ignored (`router:443` and `router:8443` both count as "router"). This is what makes the typical "LuCI on 443 / daemon on 8443" setup work without extra config.
- Strict mode: configure `list allowed_origin_host` in `/etc/config/ubus-ws`. When set, that list becomes the only authority — no fallback. Match is by hostname only.
- Missing Origin: allowed (non-browser clients like curl don't send it; Bearer still gates).
- See `ws_origin_ok()` + `hostname_from_origin()` + `hostname_from_host()`.

### 3. DoS posture

libwebsockets enforces protocol-level guards (frame size against `rx_buffer_size`, UTF-8 validation, control-frame size rules, fragmentation handling, masking). We add application-level guards as `#define`s at the top of `ws.c`:

- **Per-connection subscription cap**: 64. Reject further `subscribe` with JSON-RPC `-32005`.
- **Outbound backlog**: 1 MiB queued. On overflow we set `PENDING_TIMEOUT_USER_OK` / `LWS_TO_KILL_ASYNC` to ask libwebsockets to drop the connection cleanly.
- **Max single inbound frame**: 1 MiB via `rx_buffer_size` on the protocols entry — libwebsockets enforces.
- **Max JSON nesting depth**: 32. Pre-flight scan (`json_depth_ok`) rejects pathologically nested payloads before json-c's recursive parser ever touches them.

### 4. Async ubus dispatch

`ws_dispatch_call` uses `ubus_invoke_async` + `ubus_complete_request_async`, NOT the synchronous `ubus_invoke`. Sync invocation would block the daemon's entire uloop for up to the call's timeout (30s) — every other client, every other connection frozen. Per-connection state tracks in-flight calls (`c->pending_calls`); teardown sets `pending_call->conn = NULL` so completion callbacks drop the reply rather than writing to a freed connection.

`ws_acl_check` and `ws_authenticate` still call `ubus_invoke` synchronously, but with 1s timeouts — the blocking is bounded.

## Wire protocol (mirrors `/ubus` POST JSON-RPC)

```jsonc
// Client → server
{"jsonrpc":"2.0","id":1,"method":"call",       "params":[obj, fn, args]}
{"jsonrpc":"2.0","id":2,"method":"list",       "params":[glob?]}
{"jsonrpc":"2.0","id":3,"method":"subscribe",  "params":[obj_path]}
{"jsonrpc":"2.0","id":4,"method":"unsubscribe","params":[obj_path]}

// Server → client
{"jsonrpc":"2.0","id":1,"result": [code, data] | "error":{code,message}}
{"jsonrpc":"2.0",        "method":"notify",    "params":[obj_path, type, data]}
```

Server-initiated `notify` has no `id`. Replies always carry the same `id` as the request.

## Project layout

```
Makefile               OpenWrt package descriptor (PKG_NAME, DEPENDS, install/postinst rules)
src/
  Makefile             Inner build (ws.o + main.o -> ubus-wsd binary, links the libs)
  ws.h                 Public interface: struct ws_config, ws_init, ws_shutdown
  ws.c                 Daemon implementation: lws callbacks, auth, ACL, async ubus
                       dispatch, subscriber lifecycle, JSON-RPC parsing
  main.c               Process bootstrap: getopt, UCI config load, signals,
                       uloop_init/run, ws_init/ws_shutdown
files/                 Tree copied verbatim into the IPK at install time
  etc/
    config/ubus-ws     Default UCI config (port 8443, cert/key paths,
                       optional allowed_origin_host allowlist)
    init.d/ubus-ws     procd-managed service definition
  usr/share/rpcd/acl.d/
    ubus-wsd.json      Grants read access to uci.ubus-ws for the JS client
  www/luci-static/resources/
    rpc-ws.js          LuCI client wrapper. Promise-based call/list +
                       callback-based subscribe; multiplexed over one WS;
                       reads daemon port via uci.load('ubus-ws')
CLAUDE.md              This file.
LICENSE                Apache-2.0.
```

## UCI config schema (`/etc/config/ubus-ws`)

```
config server 'main'
    option port               '8443'             # TLS listen port
    option cert               '/etc/uhttpd.crt'  # PEM cert (shared with uhttpd by default)
    option key                '/etc/uhttpd.key'  # PEM private key
    option ubus_socket        '/var/run/...'     # optional override; unset = libubus default
    list   allowed_origin_host 'router.lan'      # optional; when set, becomes strict allowlist
```

## procd init (`/etc/init.d/ubus-ws`)

Standard procd template. `procd_add_reload_trigger 'ubus-ws'` means `uci commit ubus-ws` + `/etc/init.d/ubus-ws reload` picks up config changes without a restart cycle for adjacent services.

## Operational notes

- `logread -e ubus-wsd` shows the startup banner ("listening on :8443") and per-connection auth-reject reasons.
- `ubus list` should show no entries from us — we're a CLIENT of ubus, not a publisher.
- The package's `postinst` does `/etc/init.d/rpcd reload` to make rpcd pick up our new ACL file, then enables + starts the daemon. `prerm` stops and disables.

## What this repo does NOT contain

- A reference producer daemon — that pattern is per-app and lives wherever the consuming app lives.
- Any uhttpd code, headers, or build artifacts. The earlier vendored `uhttpd.h`/`plugin.h`/`utils.h` are gone.

## Commit conventions

- Short, descriptive subject line; component prefix optional but welcome (e.g., `ws.c:`, `main.c:`).
- Real-name `Signed-off-by:` required for upstream merge paths.
