# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in this repository.

## What this project is

`uhttpd-mod-ws` is a **uhttpd plugin** that adds an RFC 6455 WebSocket transport for ubus JSON-RPC. It is the sibling of `uhttpd-mod-ubus` and speaks the same JSON-RPC 2.0 dialect with two added verbs (`subscribe` / `unsubscribe`) for server→client push.

It is **not** a standalone server. It is a `.so` plugin loaded by uhttpd at startup (`/usr/lib/uhttpd/uhttpd_ws.so`) and gets its socket lifecycle, TLS termination (for `wss://`), and uloop integration from uhttpd itself.

**Security-critical part of the implementation is delegated to libwebsockets**: RFC 6455 handshake (Sec-WebSocket-Accept hash, 101 response), frame parsing, mask removal, control-frame size enforcement, UTF-8 validation on TEXT frames, fragmentation reassembly, and close-code/reason validation are all done by libwebsockets — not by code we wrote. The handoff happens via `lws_adopt_socket_readbuf()` after uhttpd accepts the connection and our auth + Origin checks pass. OpenWrt's libwebsockets build has `LWS_WITH_ULOOP=ON`, so the library shares uhttpd's existing uloop natively — no event-loop bridge.

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

- Endpoint: `<c->ubus_prefix>-ws` — derived at plugin init from uhttpd's own `option ubus_prefix` config. For the default `/ubus`, that yields `/ubus-ws`. Sibling of `/ubus`, NOT a child: uhttpd's `path_match()` is a prefix match with `/`-boundary, so `/ubus/ws` would be intercepted by mod-ubus before reaching us. The hyphen breaks the prefix.
- Companion client: `rpc-ws.js` ships **inside this package** under `files/www/luci-static/resources/rpc-ws.js`, installed to `/www/luci-static/resources/rpc-ws.js` on the device. LuCI views consume it with `'require rpc-ws';`. The client derives its WS URL from `L.env.ubuspath` (the same LuCI config that rpc.js's direct-ubus probe uses), so one config knob configures both transports.
- Authoritative reference for the JSON-RPC dialect: LuCI's `modules/luci-base/ucode/controller/admin/index.uc` (`action_ubus`).

## Build context

This is a **standalone OpenWrt feed package**. Top-level `Makefile` is the OpenWrt package descriptor; `src/Makefile` is the inner build that produces `uhttpd_ws.so`.

```sh
# Wire this repo as a feed in your OpenWrt checkout, then:
echo "src-link mymod $(dirname $PWD)" >> feeds.conf      # or src-git URL
./scripts/feeds update mymod && ./scripts/feeds install uhttpd-mod-ws
make menuconfig                                  # select uhttpd-mod-ws with 'm'
make package/uhttpd-mod-ws/compile V=s
# IPK lands in bin/packages/<arch>/mymod/
```

There is no host-side build, no test suite, and no `make check`. Verification is reading + on-device testing.

### How we get uhttpd's plugin headers

uhttpd has no `Build/InstallDev`, and trying to copy them from `$(BUILD_DIR)/uhttpd-*/` at `Build/Prepare` time fails because OpenWrt cleans the source tree after each dependency's install — by the time our package builds, the headers are gone from disk.

The clean OpenWrt-canonical answer (per [jow's forum post](https://forum.openwrt.org/t/custom-packages-exporting-headers-and-linking-custom-packages/105035)) is `Build/InstallDev` in uhttpd's Makefile. We can't add that from outside uhttpd without forking its Makefile or patching the SDK in CI, and the project's GitHub workflow uses a reusable build script across multiple packages where injecting per-package SDK patches would be invasive.

**So we bundle.** `src/uhttpd.h`, `src/plugin.h`, `src/utils.h` are vendored copies from [openwrt/uhttpd@1b624f8f](https://github.com/openwrt/uhttpd/commit/1b624f8f814ed568608d756512892416e0431d77) (master as of 2026-05-17). uhttpd's plugin ABI has been effectively stable since ~2013 — these three files change very rarely.

Three things to keep aligned with upstream uhttpd, watched manually when bumping the vendored copies:

1. **`utils.h`** — pulled in by `uhttpd.h` via a relative `#include "utils.h"`. Only system headers from there, no further uhttpd internals.
2. **`HAVE_TLS`** — set in `Build/Compile` via `-DHAVE_TLS`. uhttpd in openwrt builds with this flag set, which adds a `struct ustream_ssl ssl` member to `struct client`. Fields we access (`request`, `hdr`, `dispatch`) come AFTER it, so the struct offsets only match if our compile also defines it. (`HAVE_UBUS` and `HAVE_UCODE` add fields only at the *end* of structs we use, so omitting them doesn't shift our offsets and we save a dep.)
3. **`+libustream-ssl`** — added to `DEPENDS` because `<libubox/ustream-ssl.h>` is what `HAVE_TLS` pulls in. The virtual `libustream-ssl` package is provided by `libustream-mbedtls`, `libustream-openssl`, or `libustream-wolfssl`.

**Long-term cleanup:** when an upstream PR adds `Build/InstallDev` to openwrt's `package/network/services/uhttpd/Makefile` and lands on a release branch we target, delete the three bundled headers from `src/`, drop `-DHAVE_TLS` (since uhttpd's installed headers will carry the right config), and `-I$(STAGING_DIR)/usr/include/uhttpd` will Just Work.

## Required dependencies (runtime)

- `uhttpd` (host plugin loader)
- `libubus` (RPC to ubusd)
- `libubox` (uloop, blobmsg, avl, list)
- `libblobmsg-json`, `libjson-c` (JSON ↔ blob translation)
- `libwebsockets` (RFC 6455 framing + handshake). DEPENDS uses the virtual `libwebsockets` name; satisfied by **`libwebsockets-mbedtls`** (PROVIDES `libwebsockets`) or **`libwebsockets-full`** (PROVIDES `libwebsockets`). The standalone `libwebsockets-openssl` variant does NOT provide the virtual name — users wanting an openssl backend should install `libwebsockets-full` instead.
- `rpcd` (provides the `session` ubus object — auth + ACL gating)

## Critical design constraints

These are not optional. Honor them when modifying code.

### 1. UAF rule — every `ubus_subscriber` torn down from `LWS_CALLBACK_CLOSED`

Every per-connection `ubus_subscriber` registered with ubusd must be unregistered from `LWS_CALLBACK_CLOSED` via `ws_conn_teardown()`. If you only clean up from the ubusd-side `obj.remove_cb`, the subscriber outlives the client and ubusd's reaper dereferences a freed pointer ~30s later (silent crash, procd respawn).

This is the exact bug shipped in `uhttpd-mod-ubus`'s SSE path (`/ubus/subscribe/<obj>`) as of OpenWrt 25.12. Do not replicate.

(Pre-libwebsockets we wired the teardown to `cl->dispatch.free`. The new path uses libwebsockets's own callback semantics: `LWS_CALLBACK_CLOSED` fires when the wsi is being closed, before per_session_data is freed. `LWS_CALLBACK_WSI_DESTROY` handles the disconnect-before-ESTABLISHED race for any opaque sid we stashed pre-handshake.)

### 2. Auth — two transports, sid bound at upgrade

- Primary: `Sec-WebSocket-Protocol: ubus-json-rpc-v1, bearer.<sid>` (the only way browser `WebSocket` constructors can pass auth — no header parameter).
- Fallback: `Authorization: Bearer <sid>` (curl/scripts/tests).
- Sid is validated once against `rpcd session.access` at upgrade. Per-op ACL is re-checked at dispatch time. Sid binding lives in `struct ws_conn::sid[33]` and is never reset for the life of the connection.

### 2a. Origin — CSWSH defense in depth

The upgrade handshake also validates the `Origin:` header (same-origin policy: host[:port] portion must match `Host:`). Missing `Origin:` is allowed (non-browser clients don't send it; Bearer still gates). Scheme match (https/wss vs http/ws) is not enforced server-side — browsers refuse mixed-content WS opens, so the path is already closed at the browser. See `ws_origin_ok()`.

### 3. Coexistence with mod-ubus

mod-ws does **not** patch, replace, or override mod-ubus. Both can be installed. Their URL prefixes are disjoint by construction. The `:subscribe` ACL function name is shared semantics — same `session.access` grant unlocks both transports (a feature, not a conflict).

### 4. DoS posture

libwebsockets enforces several protocol-level guards automatically (frame size against `rx_buffer_size`, UTF-8 validation, control-frame size rules, malformed frames). We still keep application-level guards as `#define`s at the top of `ws.c`:

- **Per-connection subscription cap**: 64 (configurable). Reject further `subscribe` with JSON-RPC `-32005`.
- **Outbound backlog**: 1 MiB queued. On overflow we set `PENDING_TIMEOUT_USER_OK` / `LWS_TO_KILL_ASYNC` to ask libwebsockets to drop the connection cleanly.
- **Max single inbound frame**: 1 MiB via `rx_buffer_size` on the protocols entry — libwebsockets enforces.
- **Liveness**: libwebsockets has built-in PING/PONG bookkeeping; tune via `lws_context_creation_info::ws_ping_pong_interval` if you want stricter than the default.

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
Makefile        OpenWrt package descriptor. Names the package, declares
                DEPENDS (incl. the libwebsockets OR-condition), drives
                src/Makefile via Build/Compile, copies files/ into the
                IPK at install time.
src/
  Makefile      Inner build. Compiles ws.c into uhttpd_ws.so, links
                libubus + libubox + libblobmsg-json + libjson-c +
                libwebsockets. CC/CFLAGS/LDFLAGS injected by outer
                Makefile.
  ws.c          Main implementation. Auth + Origin + ubus glue is ours;
                RFC 6455 framing/handshake is libwebsockets. Headers
                explain each section.
files/          Tree copied verbatim into the IPK at install time.
                Mirrors device paths -- files/www/... -> /www/... etc.
  www/luci-static/resources/
    rpc-ws.js   LuCI client wrapper. Promise-based call/list +
                callback-based subscribe; multiplexed over one WS;
                reads L.env.ubuspath for the WS URL.
CLAUDE.md       This file.
```

## Open verification items

Mechanical to resolve once a build is running on a device:

1. **libwebsockets ↔ uloop integration**: `ws_lws_init()` creates the context with `LWS_SERVER_OPTION_VALIDATE_UTF8 | LWS_SERVER_OPTION_DISABLE_IPV6_LISTEN`. With `LWS_WITH_ULOOP=ON` in the OpenWrt build, lws *should* use uhttpd's existing uloop automatically, but if there's a flag to opt in (`LWS_SERVER_OPTION_ULOOP` or via `foreign_loops`), add it. Verify by watching whether wsi callbacks fire — if they don't, lws isn't seeing uloop events.
2. **`cl->request.url` field name**: `ws_rebuild_request()` reads it to construct the request line for `lws_adopt_socket_readbuf`. May actually be `cl->request.uri` or similar in some uhttpd versions. The plugin won't accept WS upgrades if this is wrong; check the uhttpd plugin.h.
3. **`cl->fd.fd` and `ops->client_close()` semantics**: we `dup()` the fd before adopt and `ops->client_close(cl)` afterwards. Verify uhttpd doesn't double-close or try to read after.
4. **Session id re-lookup**: `session_obj_id` is cached at plugin init; if rpcd isn't up yet at uhttpd startup it's logged-but-not-fatal. Add a retry on first authenticate if you see "session not found" errors.

## What this repo does NOT contain

- A reference producer daemon (`publish()` + `subscribe_cb` lifecycle) — that pattern is per-app and lives wherever the consuming app lives. See `applications/luci-app-example/` in openwrt/luci for a small reference.
- A hard `luci-base` dependency — the package installs `rpc-ws.js` into `/www/luci-static/resources/` regardless. Harmless when LuCI isn't installed; immediately useful when it is. If you want to force LuCI as a prerequisite, add `+luci-base` to `DEPENDS`.

## Commit conventions

- Subject prefix: short, lowercase after prefix (matches OpenWrt feed convention).
- Real-name `Signed-off-by:` required for upstream merge paths (CI checks this on openwrt/* repos).
