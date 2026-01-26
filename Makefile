CC = gcc
CFLAGS = -O6 -Wall -Wextra -pedantic
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
