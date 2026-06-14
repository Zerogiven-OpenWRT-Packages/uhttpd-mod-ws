# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## What this project is

`uhttpd-mod-ws` is a **uhttpd plugin** that adds an RFC 6455 WebSocket transport for ubus JSON-RPC. It is the sibling of `uhttpd-mod-ubus` and speaks the same JSON-RPC 2.0 dialect with two added verbs (`subscribe` / `unsubscribe`) for server→client push.

It is **not** a standalone server. It is a `.so` plugin loaded by uhttpd at startup (`/usr/lib/uhttpd/uhttpd_ws.so`) and gets its socket lifecycle, TLS termination (for `wss://`), and uloop integration from uhttpd itself.

## Where it fits

```
  Browser ──wss──▶ uhttpd ──dlopen──▶ uhttpd_ws.so ──libubus──▶ ubusd
                     │                                            │
                     ├──▶ uhttpd_ubus.so   (POST /ubus, SSE)      │
                     ├──▶ uhttpd_ucode.so  (forked ucode CGI)     │
                     └──▶ uhttpd_lua.so    (forked Lua CGI)       │
                                                                  │
                                                          rpcd ───┘
                                                          (session.access ACL)
```

- Endpoint: `/ubus-ws` — sibling of `/ubus`, NOT a child. uhttpd's `path_match()` is a prefix match with `/`-boundary, so `/ubus/ws` would be intercepted by mod-ubus before reaching us. The hyphen breaks the prefix.
- Companion client: `rpc-ws.js` lives in **openwrt/luci** (`modules/luci-base/htdocs/luci-static/resources/rpc-ws.js`). Not in this repo.
- Authoritative reference for the JSON-RPC dialect: LuCI's `modules/luci-base/ucode/controller/admin/index.uc` (`action_ubus`).

## Build context

This is **not built standalone**. It compiles inside an OpenWrt buildroot, either:

1. **Patched into the uhttpd source tree** at `package/network/services/uhttpd/src/ws.c` with a sub-package stanza in uhttpd's `Makefile` (see `Makefile.patch` in this repo) — this is the upstream-merge shape.
2. **As a standalone feed package** that depends on uhttpd headers — requires more Makefile scaffolding but doesn't touch the uhttpd source.

```sh
# Inside an OpenWrt checkout with this repo wired as a feed (option 2):
./scripts/feeds update && ./scripts/feeds install uhttpd-mod-ws
make menuconfig                # select uhttpd-mod-ws with 'm'
make package/uhttpd-mod-ws/compile
# IPK lands in bin/packages/<arch>/<feed>/
```

There is no host-side build, no test suite, and no `make check`. Verification is reading + on-device testing.

## Required dependencies (runtime)

- `uhttpd` (host plugin loader)
- `libubus` (RPC to ubusd)
- `libubox` (uloop, ustream, blobmsg, avl)
- `libblobmsg-json`, `libjson-c` (JSON ↔ blob translation)
- `rpcd` (provides the `session` ubus object — auth + ACL gating)

## Critical design constraints

These are not optional. Honor them when modifying code.

### 1. UAF rule — `cl->dispatch.free` is mandatory

Every per-connection `ubus_subscriber` registered with ubusd must be unregistered from `cl->dispatch.free`. If you only clean up from the ubusd-side `obj.remove_cb`, the subscriber outlives the client and ubusd's reaper dereferences a freed pointer ~30s later (silent crash, procd respawn).

This is the exact bug shipped in `uhttpd-mod-ubus`'s SSE path (`/ubus/subscribe/<obj>`) as of OpenWrt 25.12. Do not replicate.

Wired in `ws_conn_free_cb` (the body iterates `c->subs` and calls `ubus_unregister_subscriber` for each).

### 2. Auth — two transports, sid bound at upgrade

- Primary: `Sec-WebSocket-Protocol: ubus-json-rpc-v1, bearer.<sid>` (the only way browser `WebSocket` constructors can pass auth — no header parameter).
- Fallback: `Authorization: Bearer <sid>` (curl/scripts/tests).
- Sid is validated once against `rpcd session.access` at upgrade. Per-op ACL is re-checked at dispatch time. Sid binding lives in `struct ws_conn::sid[33]` and is never reset for the life of the connection.

### 3. Coexistence with mod-ubus

mod-ws does **not** patch, replace, or override mod-ubus. Both can be installed. Their URL prefixes are disjoint by construction. The `:subscribe` ACL function name is shared semantics — same `session.access` grant unlocks both transports (a feature, not a conflict).

### 4. DoS posture

V1 ships with three guards, all configurable as `#define`s at the top of `ws.c`:

- **Liveness**: `PING` every 20s; close if no `PONG` in 45s (under uhttpd's ~30s `network_timeout`).
- **Per-connection subscription cap**: 64. Reject further `subscribe` with `-32005`.
- **Outbound backlog watchdog**: close `1009` if ustream pending > 1 MiB (slow-consumer protection).
- **Inbound frame cap**: 1 MiB (close `1009`); fragmented frames rejected (close `1003`).

Adjust the constants for the deployment but do not remove the guards.

## Wire protocol (Candidate α, mirrors `/ubus`)

```jsonc
// Client → server (all have id; replies correlate by id)
{"jsonrpc":"2.0","id":1,"method":"call",       "params":[obj, fn, args]}
{"jsonrpc":"2.0","id":2,"method":"list",       "params":[glob?]}
{"jsonrpc":"2.0","id":3,"method":"subscribe",  "params":[obj_path]}
{"jsonrpc":"2.0","id":4,"method":"unsubscribe","params":[obj_path]}

// Server → client
{"jsonrpc":"2.0","id":1,"result": [code, data] | "error":{code,message}}
{"jsonrpc":"2.0",        "method":"notify",    "params":[obj_path, type, data]}
```

Server-initiated `notify` has no `id`. Replies always have the same `id` as the request.

## Project layout

```
ws.c            Main implementation. Headers explain each section.
sha1.c          Steve Reid's public-domain SHA-1 (needed for the
                Sec-WebSocket-Accept handshake hash). To be added.
base64.c        Tiny RFC 4648 base64 encoder. To be added (or extract
                from libubox/utils.c if exported there).
Makefile.patch  Notional unified diff against uhttpd's Makefile for the
                in-tree integration path. Adjust line offsets to apply.
```

## Open verification items

Mechanical to resolve once a build is running on a device:

1. `cl->us->w.data_bytes` in `ws_pending_bytes()` — this is the assumed accessor for ustream's outbound pending byte count. The exact field/helper may differ on TLS-wrapped connections (where `cl->us` wraps an `ustream_ssl`). Adjust if `1009`-close fires spuriously.
2. Whether `session_obj_id` should be re-looked-up on first auth if the initial lookup at plugin init failed (rpcd may not be running yet at uhttpd startup). The code currently logs and continues; add a retry if you see "session not found" errors after rpcd starts.

## What this repo does NOT contain

- The browser-side client (`rpc-ws.js`) — lives in openwrt/luci.
- A reference producer daemon (`publish()` + `subscribe_cb` lifecycle) — that pattern is per-app and lives wherever the consuming app lives. See `applications/luci-app-example/` in openwrt/luci for a small reference.
- An IPK Makefile — this is currently a patch against uhttpd's Makefile. If repackaged as a standalone feed package, add a full OpenWrt Makefile.

## Commit conventions (if upstreaming via openwrt/openwrt)

- Subject prefix: `uhttpd:` (since we land as a sub-package of uhttpd).
- Real-name `Signed-off-by:` required.
- One logical change per commit; the C source + the Makefile addition typically go in one commit.
