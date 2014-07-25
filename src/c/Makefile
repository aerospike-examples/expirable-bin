###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

AS_HOST := 127.0.0.1
AS_PORT := 3000
AS_ARGS := -h $(AS_HOST) -p $(AS_PORT)

OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CFLAGS = -std=gnu99 -g -Wall -fPIC
CFLAGS += -fno-common -fno-strict-aliasing
CFLAGS += -march=nocona -DMARCH_$(ARCH)
CFLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
CFLAGS += -D_DARWIN_UNLIMITED_SELECT
else
CFLAGS += -rdynamic
endif


CFLAGS += -I/usr/include

LDFLAGS += -lssl -lcrypto -lpthread

ifeq ($(OS),Darwin)
LDFLAGS += -L/usr/local/lib
else
LDFLAGS += -lrt
endif

LUA_CPATH += $(or \
    $(wildcard /usr/include/lua-5.1), \
    $(wildcard /usr/include/lua5.1))

ifeq ($(OS),Darwin)
LUA_LIBPATH += $(or \
    $(wildcard /usr/local/lib/liblua.5.1.dylib), \
    $(wildcard /usr/local/lib/liblua.5.1.a), \
    $(wildcard /usr/local/lib/liblua.dylib), \
    $(wildcard /usr/local/lib/liblua.a), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
else
# Linux
LUA_LIBPATH += $(or \
    $(wildcard /usr/lib/liblua5.1.so), \
    $(wildcard /usr/lib/liblua5.1.a), \
    $(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.so), \
    $(wildcard /usr/lib/x86_64-linux-gnu/liblua5.1.a), \
    $(wildcard /usr/lib64/liblua-5.1.so), \
    $(wildcard /usr/lib64/liblua-5.1.a), \
    $(wildcard /usr/lib/liblua.so), \
    $(wildcard /usr/lib/liblua.a), \
	$(error Cannot find liblua 5.1) \
    )
LUA_LIBDIR = $(dir LUA_LIBPATH)
LUA_LIB = $(patsubst lib%,%,$(basename $(notdir $(LUA_LIBPATH))))
endif

CFLAGS += $(LUA_CPATH:%:-I%)
LDFLAGS += -L$(LUA_LIBDIR) -l$(LUA_LIB)

LDFLAGS += -lm
LDFLAGS += -lz

# ifeq ($(OS),Darwin)
# LDFLAGS += -lz
# endif

ifeq ($(OS),Darwin)
CC = clang
else
CC = gcc
endif

###############################################################################
##  MAIN TARGETS                                                             ##
###############################################################################

all: build

.PHONY: build
build: target/expire_bin

.PHONY: clean
clean:
	@rm -rf target

target:
	mkdir $@

target/expire_bin: expire_bin.c | target
	$(CC) $(CFLAGS) -o $@ $^ /usr/lib/libaerospike.a $(LDFLAGS)

.PHONY: run
run: build
	./target/expire_bin


