'use strict';
'require baseclass';
'require rpc';

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
 * Requires `uhttpd-mod-ws` on the device (sibling of `uhttpd-mod-ubus`).
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
 * The WS endpoint lives at the uhttpd dispatch level, NOT under LuCI's
 * /cgi-bin/luci/admin/* tree. The path follows uhttpd's ubus_prefix:
 * mod-ubus serves /<prefix>, mod-ws serves /<prefix>-ws.
 *
 * We reuse L.env.ubuspath (populated by luci-base from /etc/config/luci's
 * option ubuspath, default '/ubus/') as the source of truth -- one config
 * knob configures both rpc.js's direct path probe and rpc-ws.js's WS URL.
 */
const WS_URL = (function () {
	const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
	const httpPath = (L.env.ubuspath ?? '/ubus/').replace(/\/+$/, '');
	return scheme + '//' + location.host + httpPath + '-ws';
})();

const SUBPROTO = 'ubus-json-rpc-v1';

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
		const sid = rpc.getSessionID();
		const ws  = new WebSocket(WS_URL, [SUBPROTO, 'bearer.' + sid]);

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
