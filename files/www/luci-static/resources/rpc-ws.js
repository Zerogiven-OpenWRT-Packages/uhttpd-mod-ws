'use strict';
'require baseclass';
'require rpc';
'require uci';

/**
 * @class rpc-ws
 * @memberof LuCI
 * @hideconstructor
 * @classdesc
 *
 * `LuCI.rpc-ws` is a WebSocket transport for the same JSON-RPC dialect
 * `LuCI.rpc` speaks over POST, with added subscribe/unsubscribe verbs
 * for server-to-client push (live logs, stats, events).
 *
 * Requires the `ubus-wsd` daemon installed on the device. The daemon's
 * listen port is read at runtime from /etc/config/ubus-ws via uci.load().
 *
 * Usage:
 *
 *     'require rpc-ws';
 *
 *     const conn = rpcws.connect();
 *
 *     // request/reply (Promise):
 *     conn.call('luci.podman.containers', 'list', {})
 *         .then(([code, data]) => { ... });
 *
 *     // subscribe (callback fires for each notify):
 *     const sub = conn.subscribe('luci.podman.logs.abc123', (n) => {
 *         // n = { path, type, data }
 *     });
 *     sub.cancel();
 *
 *     // tear down everything:
 *     conn.close();
 *
 * One Connection multiplexes many calls + many subscriptions over a
 * single WS. App authors should keep one Connection per view rather
 * than opening one per subscription.
 */

/*
 * ubus-wsd listens on its own port (separate from uhttpd). We discover
 * that port at runtime by reading /etc/config/ubus-ws via uci.load(),
 * which goes through LuCI's standard uci ubus call (HTTPS to uhttpd ->
 * rpcd uci.get). The result is cached for the page lifetime.
 *
 * The URL is constructed as wss://<same-host>:<port>/ubus-ws -- the
 * daemon's URL endpoint is fixed in ws.c as WS_URL_PATH.
 */

const SUBPROTO     = 'ubus-json-rpc-v1';
const WS_URL_PATH  = '/ubus-ws';
const DEFAULT_PORT = 8443;

/* Module-level cache: avoid re-fetching UCI for every new Connection. */
let _wsUrlPromise = null;

function getWsUrl() {
	if (_wsUrlPromise) return _wsUrlPromise;
	_wsUrlPromise = uci.load('ubus-ws').then(() => {
		let port = parseInt(uci.get('ubus-ws', 'main', 'port'), 10);
		if (!port || port <= 0 || port > 65535) port = DEFAULT_PORT;
		const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
		return `${scheme}//${location.hostname}:${port}${WS_URL_PATH}`;
	}).catch(() => {
		/* If uci.load fails (rpcd ACL missing, network blip, ubus-ws
		 * package not installed yet) fall back to the default port. */
		const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
		return `${scheme}//${location.hostname}:${DEFAULT_PORT}${WS_URL_PATH}`;
	});
	return _wsUrlPromise;
}

const Connection = baseclass.extend(/** @lends LuCI.rpc-ws.Connection.prototype */ {
	__init__() {
		this.ws        = null;
		this.nextId    = 1;
		this.pending   = {};        /* id -> { resolve, reject }              */
		this.subs      = {};        /* path -> { handlers: [], serverSubbed } */
		this.outbox    = [];        /* frames queued while !OPEN              */
		this.closing   = false;
		this.backoffMs = 500;       /* current reconnect delay                */
		this._open();
	},

	/* ---- internals -------------------------------------------------- */

	_open() {
		if (this.closing) return;
		/* URL discovery is async (uci.load round-trip on first call). The
		 * outbox queue already handles calls that happen before the WS is
		 * open, so callers can fire-and-forget through call()/subscribe()
		 * immediately after rpcws.connect() returns. */
		getWsUrl().then(url => {
			if (this.closing) return;
			const sid = rpc.getSessionID();
			const ws  = new WebSocket(url, [SUBPROTO, 'bearer.' + sid]);
			this._wireWs(ws);
		}).catch(err => {
			/* URL discovery itself failed -- log and schedule a retry; the
			 * outbox will drain when a future _open() succeeds. */
			console.error('rpc-ws: URL discovery failed:', err);
			if (!this.closing) this._scheduleReconnect();
		});
	},

	/* Attach event handlers to a freshly-constructed WebSocket. Split out
	 * of _open so the async fork stays small. */
	_wireWs(ws) {
		ws.onopen = () => {
			this.backoffMs = 500;
			/* flush queued frames */
			this.outbox.forEach(s => ws.send(s));
			this.outbox = [];
			/* re-subscribe everything (post-reconnect resume) */
			for (const path in this.subs) {
				this.subs[path].serverSubbed = false;
				this._raw({
					jsonrpc: '2.0',
					id: this.nextId++,
					method: 'subscribe',
					params: [path]
				});
			}
		};

		ws.onmessage = (ev) => this._onframe(ev.data);

		ws.onclose = () => {
			/* reject in-flight calls; subs survive across reconnect */
			for (const id in this.pending)
				this.pending[id].reject(new Error('WS closed'));
			this.pending = {};

			if (!this.closing)
				this._scheduleReconnect();
		};

		ws.onerror = () => { /* close handler does the heavy lifting */ };

		this.ws = ws;
	},

	_scheduleReconnect() {
		/*
		 * TODO(you) -- DESIGN DECISION #2: reconnect backoff.
		 *
		 * Current: exponential, starts 500ms, doubles, caps at 30s.
		 * Reset to 500ms on successful onopen.
		 *
		 * Trade-offs:
		 *   - Fast (fixed 1s):  good UX during brief blips; bad for a
		 *     genuinely-down server (request storm).
		 *   - Exponential:      kinder to a struggling server; user
		 *     waits ~30s on the worst case.
		 *   - With jitter:      avoids reconnect-thundering-herd if many
		 *     views in many tabs all dropped at once.
		 *
		 * The default below is "exponential, no jitter". Add jitter or
		 * change the cap to whatever fits your deployment posture.
		 */
		setTimeout(() => this._open(), this.backoffMs);
		this.backoffMs = Math.min(this.backoffMs * 2, 30000);
	},

	_raw(obj) {
		const s = JSON.stringify(obj);
		if (this.ws && this.ws.readyState === WebSocket.OPEN)
			this.ws.send(s);
		else
			this.outbox.push(s);
	},

	_onframe(text) {
		let msg;
		try { msg = JSON.parse(text); }
		catch { return; }

		/* server-pushed notify: no id, method = "notify" */
		if (msg.method === 'notify' && Array.isArray(msg.params)) {
			const [path, type, data] = msg.params;
			const entry = this.subs[path];
			if (entry && entry.handlers.length) {
				const n = { path, type, data };
				entry.handlers.forEach(h => { try { h(n); } catch {} });
			}
			return;
		}

		/* reply to a call/list/subscribe/unsubscribe */
		if (msg.id != null) {
			const p = this.pending[msg.id];
			if (!p) return;
			delete this.pending[msg.id];
			if (msg.error) p.reject(Object.assign(
				new Error(msg.error.message ?? 'RPC error'),
				{ code: msg.error.code }));
			else p.resolve(msg.result);
		}
	},

	_request(method, params) {
		return new Promise((resolve, reject) => {
			const id = this.nextId++;
			this.pending[id] = { resolve, reject };
			this._raw({ jsonrpc: '2.0', id, method, params });
		});
	},

	/* ---- public API ------------------------------------------------- */

	/**
	 * Invoke a ubus method.
	 * @param {string} object
	 * @param {string} method
	 * @param {object} [params={}]
	 * @returns {Promise<[number, object]>} `[code, data]` mirroring /ubus
	 */
	call(object, method, params) {
		return this._request('call', [object, method, params ?? {}]);
	},

	/**
	 * List ubus objects (optionally filtered by glob).
	 * @param {string} [pattern]
	 * @returns {Promise<object>}
	 */
	list(pattern) {
		return this._request('list', pattern ? [pattern] : []);
	},

	/**
	 * Subscribe to push notifications from a ubus object.
	 *
	 * TODO(you) -- DESIGN DECISION #3: shared-subscription semantics.
	 *
	 * What happens when subscribe(path, h2) is called and (path, h1) is
	 * already active?
	 *
	 *   (a) Multiplex handlers, single server subscription. Both h1 and
	 *       h2 fire for each notify. cancel() decrements; last cancel
	 *       sends server unsubscribe. THIS IS WHAT THE STUB BELOW DOES.
	 *       Good when two widgets in the same view want the same stream.
	 *
	 *   (b) Replace h1 with h2. Simple; surprising if app forgets a sub.
	 *
	 *   (c) Throw / return null. Forces caller to coordinate. Most rigid,
	 *       worst for composability.
	 *
	 * I went with (a) as the safer default. Confirm or change.
	 *
	 * @param {string} path - exact ubus object path (no globs)
	 * @param {(notify: {path,type,data}) => void} handler
	 * @returns {{cancel: () => void, path: string}}
	 */
	subscribe(path, handler) {
		let entry = this.subs[path];
		if (!entry) {
			entry = this.subs[path] = { handlers: [], serverSubbed: false };
		}
		entry.handlers.push(handler);

		if (!entry.serverSubbed) {
			entry.serverSubbed = true;
			this._request('subscribe', [path]).catch(err => {
				/* server-side failure (ACL, not-found, too-many) -- evict */
				entry.serverSubbed = false;
				const handlers = entry.handlers;
				delete this.subs[path];
				handlers.forEach(h => {
					try { h({ path, type: 'error', data: { error: err.message } }); }
					catch {}
				});
			});
		}

		return {
			path,
			cancel: () => {
				const e = this.subs[path];
				if (!e) return;
				const i = e.handlers.indexOf(handler);
				if (i >= 0) e.handlers.splice(i, 1);
				if (e.handlers.length === 0) {
					delete this.subs[path];
					this._request('unsubscribe', [path]).catch(() => {});
				}
			}
		};
	},

	/**
	 * Close the connection. All pending calls reject; all subscriptions drop.
	 * Once closed, a Connection cannot be reopened -- create a new one.
	 */
	close() {
		this.closing = true;
		this.subs = {};
		if (this.ws) {
			try { this.ws.close(); } catch {}
			this.ws = null;
		}
	}
});

return baseclass.extend(/** @lends LuCI.rpc-ws.prototype */ {
	Connection,

	/**
	 * Open a new multiplexed WS connection to the device.
	 *
	 * Recommended pattern: one connection per view, held in the view's
	 * load() return value or as a closure; closed in the view's teardown.
	 *
	 * @returns {LuCI.rpc-ws.Connection}
	 */
	connect() {
		return new Connection();
	}
});
