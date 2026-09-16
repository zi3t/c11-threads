# A minimal 10-thread pipeline in C11

A single-file learning example of a producer, eight processing stages, and a consumer connected by bounded queues.

```text
producer -> stage 1 -> stage 2 -> ... -> stage 8 -> consumer
```

The program demonstrates:

- `struct` and `enum`
- fixed-size arrays
- `pthread_create` and `pthread_join`
- mutexes and condition variables
- a bounded queue with backpressure
- passing context into a thread
- clean shutdown with a sentinel message

It uses the C11 language standard and POSIX threads, so it builds on Linux and macOS.

## Build and run

```bash
make
./pipeline
```

Or compile directly:

```bash
cc -std=c11 -O2 -Wall -Wextra -Wpedantic -Werror \
  -pthread pipeline.c -o pipeline
./pipeline
```

Expected output ends with:

```text
message 11: value=47 expected=47 OK
All workers stopped cleanly.
```

## How it works

Each adjacent pair of threads shares one `queue`. A producer waits when its output queue is full. A consumer waits when its input queue is empty. The queue protects its state with a mutex and uses two condition variables:

- `not_empty` wakes a waiting consumer after a push.
- `not_full` wakes a waiting producer after a pop.

Every processing stage adds its stage number to the message value. After the final data message, the producer sends `MESSAGE_STOP`. Each stage forwards that sentinel before exiting, allowing the consumer and then `main` to finish without cancellation or detached threads.

## Backpressure demonstration

The example uses a capacity of 4 and pauses stage 4 for 10 ms after it
processes each data message. This lets the queue before that stage fill, which
eventually makes upstream stages wait. At shutdown, the program reports every
channel that had one or more full-queue waits, for example:

```text
backpressure: channel 3 was full 8 time(s)
```

## Exercises

1. Change `QUEUE_CAPACITY` to `1` and `32`.
2. Move the `nanosleep` to another stage and compare the reported full queues.
3. Add a second consumer and consider whether output order remains deterministic.
4. Add timestamps to `message` and calculate end-to-end latency.
5. Replace the blocking `queue_push` with a non-blocking `queue_try_push` and count dropped messages.

Choose an open-source license and add a `LICENSE` file before publishing this folder as a standalone project.
