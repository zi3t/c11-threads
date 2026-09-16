CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Werror

.PHONY: all run clean

all: pipeline

pipeline: pipeline.c
	$(CC) $(CFLAGS) -pthread pipeline.c -o pipeline

run: pipeline
	./pipeline

clean:
	rm -f pipeline
