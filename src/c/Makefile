###############################################################################
##  SETTINGS                                                                 ##
###############################################################################

OS = $(shell uname)
ARCH = $(shell uname -m)
PLATFORM = $(OS)-$(ARCH)

CFLAGS = -std=gnu99 -g -Wall -fPIC
CFLAGS += -fno-common -fno-strict-aliasing
CFLAGS += -march=nocona -DMARCH_$(ARCH)
CFLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE

ifeq ($(OS),Darwin)
  CFLAGS += -D_DARWIN_UNLIMITED_SELECT
  TARGET_LIB= /usr/local/lib
else
  CFLAGS += -rdynamic
  TARGET_LIB= /usr/lib
endif

CFLAGS += -I/usr/local/include -I/usr/local/include/ck
CFLAGS += -I src/include

LDFLAGS = -lssl -lcrypto -lpthread
ifneq ($(OS),Darwin)
  LDFLAGS += -lrt -ldl
endif

LDFLAGS += -lm

ifeq ($(OS),Darwin)
  CC = clang
else
  CC = gcc
endif

###############################################################################
##  OBJECTS                                                                  ##
###############################################################################

OBJECTS = expire_bin.o

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

target/obj: | target
	mkdir $@

target/obj/expire_bin.o: expire_bin.c | target/obj
	$(CC) $(CFLAGS) -o $@ -c $^

target/expire_bin: $(addprefix target/obj/,$(OBJECTS)) | target
	$(CC) -o $@ $^ $(TARGET_LIB)/libaerospike.a $(LDFLAGS)

.PHONY: run
run: build
	./target/expire_bin

.PHONY: valgrind
valgrind: build
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes -v ./target/expire_bin