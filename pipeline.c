#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * A small 11-thread pipeline:
 *
 * producer -> stage 1 -> ... -> stage 8 -> consumer 1 / consumer 2
 *
 * Each arrow is a bounded, thread-safe queue. A STOP message travels through
 * the pipeline after the final DATA message so every thread can exit cleanly.
 */

enum {
  STAGE_COUNT = 8,
  CONSUMER_COUNT = 2,
  WORKER_COUNT = 1 + STAGE_COUNT + CONSUMER_COUNT,
  CHANNEL_COUNT = STAGE_COUNT + 1,
  QUEUE_CAPACITY = 4,
  MESSAGE_COUNT = 12,
  SLOW_STAGE_NUMBER = 4,
};

typedef enum {
  MESSAGE_DATA,
  MESSAGE_STOP,
} message_kind;

typedef struct {
  message_kind kind;
  size_t id;
  int value;
} message;

typedef struct {
  message items[QUEUE_CAPACITY];
  size_t read_at;
  size_t write_at;
  size_t count;
  size_t full_waits;
  pthread_mutex_t mutex;
  pthread_cond_t not_empty;
  pthread_cond_t not_full;
} queue;

typedef struct {
  size_t stage_number;
  size_t consumer_number;
  queue *input;
  queue *output;
} worker_context;

static void fail_pthread(int error, const char *operation) {
  if (error != 0) {
    fprintf(stderr, "%s failed: %s\n", operation, strerror(error));
    exit(EXIT_FAILURE);
  }
}

static void queue_init(queue *q) {
  *q = (queue){0};
  fail_pthread(pthread_mutex_init(&q->mutex, NULL), "pthread_mutex_init");
  fail_pthread(pthread_cond_init(&q->not_empty, NULL), "pthread_cond_init");
  fail_pthread(pthread_cond_init(&q->not_full, NULL), "pthread_cond_init");
}

static void queue_destroy(queue *q) {
  fail_pthread(pthread_cond_destroy(&q->not_full), "pthread_cond_destroy");
  fail_pthread(pthread_cond_destroy(&q->not_empty), "pthread_cond_destroy");
  fail_pthread(pthread_mutex_destroy(&q->mutex), "pthread_mutex_destroy");
}

static void queue_push(queue *q, message item) {
  fail_pthread(pthread_mutex_lock(&q->mutex), "pthread_mutex_lock");

  while (q->count == QUEUE_CAPACITY) {
    ++q->full_waits;
    fail_pthread(pthread_cond_wait(&q->not_full, &q->mutex),
                 "pthread_cond_wait");
  }

  q->items[q->write_at] = item;
  q->write_at = (q->write_at + 1U) % QUEUE_CAPACITY;
  ++q->count;

  fail_pthread(pthread_cond_signal(&q->not_empty), "pthread_cond_signal");
  fail_pthread(pthread_mutex_unlock(&q->mutex), "pthread_mutex_unlock");
}

static void slow_stage(void) {
  const struct timespec delay = {
      .tv_nsec = 10 * 1000 * 1000,
  };

  while (nanosleep(&delay, NULL) == -1 && errno == EINTR) {
  }
}

static message queue_pop(queue *q) {
  fail_pthread(pthread_mutex_lock(&q->mutex), "pthread_mutex_lock");

  while (q->count == 0U) {
    fail_pthread(pthread_cond_wait(&q->not_empty, &q->mutex),
                 "pthread_cond_wait");
  }

  const message item = q->items[q->read_at];
  q->read_at = (q->read_at + 1U) % QUEUE_CAPACITY;
  --q->count;

  fail_pthread(pthread_cond_signal(&q->not_full), "pthread_cond_signal");
  fail_pthread(pthread_mutex_unlock(&q->mutex), "pthread_mutex_unlock");
  return item;
}

static void *producer_main(void *argument) {
  worker_context *context = argument;

  for (size_t id = 0U; id < MESSAGE_COUNT; ++id) {
    const message item = {
        .kind = MESSAGE_DATA,
        .id = id,
        .value = (int)id,
    };
    queue_push(context->output, item);
  }

  queue_push(context->output, (message){.kind = MESSAGE_STOP});
  return NULL;
}

static void *stage_main(void *argument) {
  worker_context *context = argument;

  for (;;) {
    message item = queue_pop(context->input);

    if (item.kind == MESSAGE_STOP) {
      queue_push(context->output, item);
      return NULL;
    }

    /* Each stage makes one visible, deterministic change. */
    item.value += (int)context->stage_number;
    if (context->stage_number == SLOW_STAGE_NUMBER) {
      slow_stage();
    }
    queue_push(context->output, item);
  }
}

static void *consumer_main(void *argument) {
  worker_context *context = argument;
  const int sum_of_stage_numbers = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8;

  for (;;) {
    const message item = queue_pop(context->input);

    if (item.kind == MESSAGE_STOP) {
      /* Relay the sentinel so every consumer can stop. */
      queue_push(context->input, item);
      return NULL;
    }

    const int expected = (int)item.id + sum_of_stage_numbers;
    /* Queue removal is FIFO, but concurrent consumers need not print in it. */
    printf("consumer %zu: message %2zu: value=%2d expected=%2d %s\n",
           context->consumer_number, item.id, item.value, expected,
           item.value == expected ? "OK" : "ERROR");
  }
}

int main(void) {
  queue channels[CHANNEL_COUNT];
  pthread_t threads[WORKER_COUNT];
  worker_context contexts[WORKER_COUNT];

  for (size_t index = 0U; index < CHANNEL_COUNT; ++index) {
    queue_init(&channels[index]);
  }

  contexts[0] = (worker_context){
      .output = &channels[0],
  };
  fail_pthread(pthread_create(&threads[0], NULL, producer_main, &contexts[0]),
               "pthread_create");

  for (size_t index = 1U; index <= STAGE_COUNT; ++index) {
    contexts[index] = (worker_context){
        .stage_number = index,
        .input = &channels[index - 1U],
        .output = &channels[index],
    };
    fail_pthread(
        pthread_create(&threads[index], NULL, stage_main, &contexts[index]),
        "pthread_create");
  }

  for (size_t index = 0U; index < CONSUMER_COUNT; ++index) {
    const size_t thread_index = 1U + STAGE_COUNT + index;
    contexts[thread_index] = (worker_context){
        .consumer_number = index + 1U,
        .input = &channels[CHANNEL_COUNT - 1U],
    };
    fail_pthread(pthread_create(&threads[thread_index], NULL, consumer_main,
                                &contexts[thread_index]),
                 "pthread_create");
  }

  for (size_t index = 0U; index < WORKER_COUNT; ++index) {
    fail_pthread(pthread_join(threads[index], NULL), "pthread_join");
  }

  for (size_t index = 0U; index < CHANNEL_COUNT; ++index) {
    if (channels[index].full_waits != 0U) {
      printf("backpressure: channel %zu was full %zu time(s)\n", index + 1U,
             channels[index].full_waits);
    }
  }

  for (size_t index = 0U; index < CHANNEL_COUNT; ++index) {
    queue_destroy(&channels[index]);
  }

  puts("All workers stopped cleanly.");
  return EXIT_SUCCESS;
}
