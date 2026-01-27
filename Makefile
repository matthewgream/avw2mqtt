
CC=gcc
CFLAGS_COMMON=-Wall -Wextra -Wpedantic
CFLAGS_STRICT=-Werror -Wcast-align -Wcast-qual \
    -Wstrict-prototypes \
    -Wold-style-definition \
    -Wcast-align -Wcast-qual -Wconversion \
    -Wfloat-equal -Wformat=2 -Wformat-security \
    -Winit-self -Wjump-misses-init \
    -Wlogical-op -Wmissing-include-dirs \
    -Wnested-externs -Wpointer-arith \
    -Wredundant-decls -Wshadow \
    -Wstrict-overflow=2 -Wswitch-default \
    -Wswitch-enum -Wundef \
    -Wunreachable-code -Wunused \
    -Wwrite-strings
CFLAGS=$(CFLAGS_COMMON) $(CFLAGS_STRICT) -O3 -fstack-protector-strong
LDFLAGS = -lmosquitto -lcurl -lxml2 -lcjson
INCLUDES = -I/usr/include/libxml2

PKG_CONFIG := $(shell which pkg-config 2>/dev/null)
ifdef PKG_CONFIG
    CFLAGS += $(shell pkg-config --cflags libxml-2.0 2>/dev/null)
    LDFLAGS = $(shell pkg-config --libs libmosquitto libcurl libxml-2.0 libcjson 2>/dev/null)
endif

TARGET = avw2mqtt
SRC = avw2mqtt.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

.PHONY: all clean install
