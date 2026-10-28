CC      ?= gcc
CFLAGS  ?= -O2 -g -Wall -Wextra -Wshadow -std=gnu11
CPPFLAGS = -Iinclude
LDFLAGS ?=

BIN   = bin
CORE  = src/protocol.c src/hashmap.c src/chat.c
PROGS = $(BIN)/server_tcp $(BIN)/server_udp $(BIN)/client $(BIN)/loadtest \
        $(BIN)/integration_test
PORT ?= 9100

.PHONY: all test clean sanitize

all: $(PROGS)

$(BIN):
	mkdir -p $(BIN)

$(BIN)/server_tcp: src/server_tcp.c $(CORE) include/*.h | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ src/server_tcp.c $(CORE) $(LDFLAGS)

$(BIN)/server_udp: src/server_udp.c $(CORE) include/*.h | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ src/server_udp.c $(CORE) $(LDFLAGS)

$(BIN)/client: src/client.c src/protocol.c include/protocol.h | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ src/client.c src/protocol.c $(LDFLAGS)

$(BIN)/loadtest: src/loadtest.c src/protocol.c include/protocol.h | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ src/loadtest.c src/protocol.c $(LDFLAGS) -lm

$(BIN)/integration_test: tests/integration_test.c src/protocol.c include/protocol.h | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ tests/integration_test.c src/protocol.c $(LDFLAGS)

# Starts a server on $(PORT), runs the tests against it, then stops it.
test: $(BIN)/server_tcp $(BIN)/integration_test
	@$(BIN)/server_tcp -b 127.0.0.1 -p $(PORT) 2>/tmp/chat_test_server.log & \
	pid=$$!; sleep 0.3; \
	$(BIN)/integration_test 127.0.0.1 $(PORT); rc=$$?; \
	kill $$pid; wait $$pid 2>/dev/null; exit $$rc

# Debug build with AddressSanitizer + UBSan.
sanitize:
	$(MAKE) clean
	$(MAKE) all CFLAGS="-O1 -g -Wall -Wextra -std=gnu11 -fsanitize=address,undefined -fno-omit-frame-pointer"

clean:
	rm -rf $(BIN)
