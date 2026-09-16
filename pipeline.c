#define _POSIX_C_SOURCE 200809L

#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * A small 10-thread pipeline:
 *
 * producer -> stage 1 -> ... -> stage 8 -> consumer
 *
 * Each arrow is a bounded, thread-safe queue. A STOP message travels through
 * the pipeline after the final DATA message so every thread can exit cleanly.
 */

enum {
  WORKER_COUNT = 10,
  CHANNEL_COUNT = WORKER_COUNT - 1,
  QUEUE_CAPACITY = 4,
  MESSAGE_COUNT = 12,
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
  pthread_mutex_t mutex;
  pthread_cond_t not_empty;
  pthread_cond_t not_full;
} queue;

typedef struct {
  size_t stage_number;
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
    fail_pthread(pthread_cond_wait(&q->not_full, &q->mutex), "pthread_cond_wait");
  }

  q->items[q->write_at] = item;
  q->write_at = (q->write_at + 1U) % QUEUE_CAPACITY;
  ++q->count;

  fail_pthread(pthread_cond_signal(&q->not_empty), "pthread_cond_signal");
  fail_pthread(pthread_mutex_unlock(&q->mutex), "pthread_mutex_unlock");
}

static message queue_pop(queue *q) {
  fail_pthread(pthread_mutex_lock(&q->mutex), "pthread_mutex_lock");

  while (q->count == 0U) {
    fail_pthread(pthread_cond_wait(&q->not_empty, &q->mutex), "pthread_cond_wait");
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
    queue_push(context->output, item);
  }
}

static void *consumer_main(void *argument) {
  worker_context *context = argument;
  const int sum_of_stage_numbers = 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8;

  for (;;) {
    const message item = queue_pop(context->input);

    if (item.kind == MESSAGE_STOP) {
      return NULL;
    }

    const int expected = (int)item.id + sum_of_stage_numbers;
    printf("message %2zu: value=%2d expected=%2d %s\n", item.id, item.value, expected,
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
  fail_pthread(pthread_create(&threads[0], NULL, producer_main, &contexts[0]), "pthread_create");

  for (size_t index = 1U; index < WORKER_COUNT - 1U; ++index) {
    contexts[index] = (worker_context){
        .stage_number = index,
        .input = &channels[index - 1U],
        .output = &channels[index],
    };
    fail_pthread(pthread_create(&threads[index], NULL, stage_main, &contexts[index]),
                 "pthread_create");
  }

  contexts[WORKER_COUNT - 1U] = (worker_context){
      .input = &channels[CHANNEL_COUNT - 1U],
  };
  fail_pthread(pthread_create(&threads[WORKER_COUNT - 1U], NULL, consumer_main,
                              &contexts[WORKER_COUNT - 1U]),
               "pthread_create");

  for (size_t index = 0U; index < WORKER_COUNT; ++index) {
    fail_pthread(pthread_join(threads[index], NULL), "pthread_join");
  }

  for (size_t index = 0U; index < CHANNEL_COUNT; ++index) {
    queue_destroy(&channels[index]);
  }

  puts("All workers stopped cleanly.");
  return EXIT_SUCCESS;
}
