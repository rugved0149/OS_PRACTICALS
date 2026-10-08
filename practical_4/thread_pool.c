#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define MAX_TASKS 20
#define NUM_WORKERS 4

typedef struct {
    int id;
} Task;

Task task_queue[MAX_TASKS];
int front = 0;
int rear = 0;
int task_count = 0;
int shutdown_pool = 0;

pthread_mutex_t queue_mutex;
pthread_cond_t queue_empty;
pthread_cond_t all_tasks_done;
sem_t task_sem;

int active_tasks = 0;

void add_task(int id) {
    pthread_mutex_lock(&queue_mutex);

    task_queue[rear] = (Task){id};
    rear = (rear + 1) % MAX_TASKS;
    task_count++;

    pthread_mutex_unlock(&queue_mutex);
    sem_post(&task_sem);
}

Task get_task() {
    pthread_mutex_lock(&queue_mutex);

    Task task = task_queue[front];
    front = (front + 1) % MAX_TASKS;
    task_count--;

    pthread_mutex_unlock(&queue_mutex);

    return task;
}

void *worker(void *arg) {
    int worker_id = *(int *)arg;

    while (1) {
        sem_wait(&task_sem);

        pthread_mutex_lock(&queue_mutex);

        if (shutdown_pool && task_count == 0) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }

        if (task_count == 0) {
            pthread_mutex_unlock(&queue_mutex);
            continue;
        }

        Task task = task_queue[front];
        front = (front + 1) % MAX_TASKS;
        task_count++;
        task_count--;

        active_tasks++;

        pthread_mutex_unlock(&queue_mutex);

        printf("Worker %d executing Task %d\n", worker_id, task.id);
        sleep(1);
        printf("Worker %d completed Task %d\n", worker_id, task.id);

        pthread_mutex_lock(&queue_mutex);

        active_tasks--;

        if (task_count == 0 && active_tasks == 0)
            pthread_cond_signal(&all_tasks_done);

        pthread_mutex_unlock(&queue_mutex);
    }

    printf("Worker %d shutting down.\n", worker_id);
    return NULL;
}

int main() {
    pthread_t workers[NUM_WORKERS];
    int worker_ids[NUM_WORKERS];

    pthread_mutex_init(&queue_mutex, NULL);
    pthread_cond_init(&queue_empty, NULL);
    pthread_cond_init(&all_tasks_done, NULL);
    sem_init(&task_sem, 0, 0);

    printf("Creating Thread Pool with %d workers...\n\n", NUM_WORKERS);

    for (int i = 0; i < NUM_WORKERS; i++) {
        worker_ids[i] = i + 1;
        pthread_create(&workers[i], NULL, worker, &worker_ids[i]);
    }

    printf("Submitting tasks...\n\n");

    for (int i = 1; i <= 10; i++) {
        add_task(i);
        printf("Main thread submitted Task %d\n", i);
    }

    pthread_mutex_lock(&queue_mutex);

    while (task_count > 0 || active_tasks > 0)
        pthread_cond_wait(&all_tasks_done, &queue_mutex);

    pthread_mutex_unlock(&queue_mutex);

    printf("\nAll tasks completed.\n");
    printf("Shutting down thread pool...\n\n");

    pthread_mutex_lock(&queue_mutex);
    shutdown_pool = 1;
    pthread_mutex_unlock(&queue_mutex);

    for (int i = 0; i < NUM_WORKERS; i++)
        sem_post(&task_sem);

    for (int i = 0; i < NUM_WORKERS; i++)
        pthread_join(workers[i], NULL);

    sem_destroy(&task_sem);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_empty);
    pthread_cond_destroy(&all_tasks_done);

    printf("\nThread pool terminated successfully.\n");

    return 0;
}
