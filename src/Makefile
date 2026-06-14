# Inner build Makefile -- driven by OpenWrt's top-level Makefile.
# Produces uhttpd_ws.so for /usr/lib/uhttpd/.

OBJS = ws.o
SO   = uhttpd_ws.so

# uhttpd plugins link against libubus + libubox + JSON libs, plus
# libwebsockets for the RFC 6455 framing/handshake (replaces what
# we used to do by hand). No own crypto -- libwebsockets handles
# Sec-WebSocket-Accept, masking, UTF-8 validation, etc.
# Path/header resolution is the responsibility of the outer OpenWrt
# Makefile (it sets CFLAGS / LDFLAGS to point at $(STAGING_DIR)).
LIBS = -lubus -lubox -lblobmsg_json -ljson-c -lwebsockets

CFLAGS  ?= -Wall -Wextra -Os
LDFLAGS ?=
# Note: when invoked standalone (rare), build-essential flags like
# -fPIC -std=gnu99 -fvisibility=hidden need to be in CFLAGS. The
# OpenWrt outer Makefile passes them in via the CFLAGS string it sets
# on the sub-make command line.

all: $(SO)

$(SO): $(OBJS)
	$(CC) -shared -Wl,-soname,$(SO) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(SO)

.PHONY: all clean
