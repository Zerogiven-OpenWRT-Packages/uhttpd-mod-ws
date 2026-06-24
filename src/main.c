/*
 * ubus-wsd -- WebSocket transport daemon for ubus JSON-RPC.
 *
 * Process bootstrap: parse UCI config (/etc/config/ubus-ws), install
 * signal handlers, init uloop + libubus + libwebsockets via ws_init(),
 * then drop into uloop_run() until SIGTERM/SIGINT. Clean shutdown via
 * ws_shutdown().
 *
 * All WS protocol logic lives in ws.c; this file is intentionally
 * minimal and contains nothing security-sensitive.
 *
 * Copyright (C) 2026  Apache-2.0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>

#include <libubox/uloop.h>
#include <uci.h>

#include "ws.h"

/* Defaults applied when /etc/config/ubus-ws is missing or has no value */
#define DEFAULT_PORT         8443
#define DEFAULT_CERT         "/etc/uhttpd.crt"
#define DEFAULT_KEY          "/etc/uhttpd.key"
#define DEFAULT_CONFIG_NAME  "ubus-ws"
#define DEFAULT_SECTION_TYPE "server"        /* match files/etc/config/ubus-ws */

/* ---- config holder, owned by main() for the daemon's lifetime ---------- */

struct daemon_config {
    int          port;
    char        *cert;            /* strdup'd from UCI -- freed at shutdown */
    char        *key;
    char        *ubus_socket;
    char       **allowed_origin_hosts;  /* NULL-terminated, all strdup'd   */
    size_t       allowed_origin_n;
};

static struct daemon_config g_cfg;

/* ---- UCI helpers ------------------------------------------------------- */

/*
 * Walk options in the first 'server' section of /etc/config/ubus-ws and
 * pull values into g_cfg. Missing options keep their defaults. Multi-value
 * 'list' options are accumulated into allowed_origin_hosts.
 */
static int
config_load(void)
{
    struct uci_context *ctx;
    struct uci_package *pkg = NULL;
    struct uci_element *sec_e, *opt_e;
    struct uci_section *section = NULL;
    int rc = -1;

    ctx = uci_alloc_context();
    if (!ctx) {
        fprintf(stderr, "ubus-wsd: uci_alloc_context failed\n");
        return -1;
    }

    if (uci_load(ctx, DEFAULT_CONFIG_NAME, &pkg) != UCI_OK || !pkg) {
        fprintf(stderr,
            "ubus-wsd: /etc/config/%s missing or unreadable; using defaults\n",
            DEFAULT_CONFIG_NAME);
        rc = 0;        /* defaults are acceptable */
        goto out;
    }

    /* First server-type section wins. */
    uci_foreach_element(&pkg->sections, sec_e) {
        struct uci_section *s = uci_to_section(sec_e);
        if (!strcmp(s->type, DEFAULT_SECTION_TYPE)) { section = s; break; }
    }
    if (!section) {
        fprintf(stderr,
            "ubus-wsd: no 'config server' section in /etc/config/%s; using defaults\n",
            DEFAULT_CONFIG_NAME);
        rc = 0;
        goto out;
    }

    uci_foreach_element(&section->options, opt_e) {
        struct uci_option *opt = uci_to_option(opt_e);
        const char *name = opt->e.name;

        if (opt->type == UCI_TYPE_STRING) {
            const char *v = opt->v.string;
            if      (!strcmp(name, "port"))           g_cfg.port = atoi(v);
            else if (!strcmp(name, "cert"))           g_cfg.cert = strdup(v);
            else if (!strcmp(name, "key"))            g_cfg.key  = strdup(v);
            else if (!strcmp(name, "ubus_socket"))    g_cfg.ubus_socket = strdup(v);
        } else if (opt->type == UCI_TYPE_LIST &&
                   !strcmp(name, "allowed_origin_host")) {
            struct uci_element *lel;
            uci_foreach_element(&opt->v.list, lel) {
                char **n = realloc(g_cfg.allowed_origin_hosts,
                                   sizeof(char *) * (g_cfg.allowed_origin_n + 2));
                if (!n) continue;
                g_cfg.allowed_origin_hosts = n;
                g_cfg.allowed_origin_hosts[g_cfg.allowed_origin_n++] = strdup(lel->name);
                g_cfg.allowed_origin_hosts[g_cfg.allowed_origin_n]   = NULL;
            }
        }
    }
    rc = 0;

out:
    uci_free_context(ctx);
    return rc;
}

static void
config_apply_defaults(void)
{
    if (g_cfg.port <= 0 || g_cfg.port > 65535) g_cfg.port = DEFAULT_PORT;
    if (!g_cfg.cert) g_cfg.cert = strdup(DEFAULT_CERT);
    if (!g_cfg.key)  g_cfg.key  = strdup(DEFAULT_KEY);
    /* ubus_socket NULL means "use libubus default" -- leave it */
    /* allowed_origin_hosts NULL means "permissive same-host" -- leave it */
}

static void
config_free(void)
{
    free(g_cfg.cert);
    free(g_cfg.key);
    free(g_cfg.ubus_socket);
    if (g_cfg.allowed_origin_hosts) {
        for (size_t i = 0; i < g_cfg.allowed_origin_n; i++)
            free(g_cfg.allowed_origin_hosts[i]);
        free(g_cfg.allowed_origin_hosts);
    }
    memset(&g_cfg, 0, sizeof(g_cfg));
}

/* ---- signal handling --------------------------------------------------- */

static void
on_signal(int sig)
{
    fprintf(stderr, "ubus-wsd: caught signal %d, shutting down\n", sig);
    uloop_end();
}

static void
install_signal_handlers(void)
{
    struct sigaction sa = { 0 };
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    /* Don't die on SIGPIPE when a client disconnects mid-write --
     * libwebsockets handles the EPIPE return value itself. */
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
}

/* ---- main -------------------------------------------------------------- */

static void
usage(const char *argv0)
{
    fprintf(stderr,
        "Usage: %s [-h]\n"
        "  WebSocket transport daemon for ubus JSON-RPC.\n"
        "  Configuration is read from /etc/config/ubus-ws (UCI).\n",
        argv0);
}

int
main(int argc, char **argv)
{
    struct ws_config wscfg;
    int opt;

    while ((opt = getopt(argc, argv, "h")) != -1) {
        switch (opt) {
        case 'h':  usage(argv[0]); return 0;
        default:   usage(argv[0]); return 1;
        }
    }

    if (config_load() < 0) return 1;
    config_apply_defaults();

    install_signal_handlers();
    uloop_init();

    /* Hand the parsed config to ws.c. The string fields are owned by
     * g_cfg and outlive ws_init's use (we don't shutdown until uloop ends). */
    wscfg.port                 = g_cfg.port;
    wscfg.cert                 = g_cfg.cert;
    wscfg.key                  = g_cfg.key;
    wscfg.ubus_socket          = g_cfg.ubus_socket;
    wscfg.allowed_origin_hosts = (const char **)g_cfg.allowed_origin_hosts;

    if (ws_init(&wscfg) < 0) {
        config_free();
        uloop_done();
        return 1;
    }

    uloop_run();

    ws_shutdown();
    uloop_done();
    config_free();
    return 0;
}
