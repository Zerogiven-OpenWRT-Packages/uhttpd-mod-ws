# Inner build Makefile -- driven by OpenWrt's top-level Makefile.
# Produces the ubus-wsd executable.

OBJS = main.o ws.o
BIN  = ubus-wsd

LIBS = -lubus -lubox -lblobmsg_json -ljson-c -lwebsockets -luci

CFLAGS  ?= -Wall -Wextra -Os
LDFLAGS ?=

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(BIN)

.PHONY: all clean
