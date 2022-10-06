#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

extern "C"
{
    #define DEFAULT_POOL_SIZE 4

    typedef void *(*func_ptr)(void *arg);
    extern uint32_t thread_id_counter;      // should be atomic
    extern pthread_mutex_t pool_mutex;

    typedef enum
    {
        IDLE = 0,
        RUNNING,
        STOPPED         // can be used to kill a thread, not needed for now
    } thread_status;

    struct my_thread_s;
    struct my_thread_pool_s;
    typedef struct my_thread_s my_thread;
    typedef struct my_thread_pool_s my_thread_pool;

    struct my_thread_s
    {
        struct my_thread_pool_s *pool;

        my_thread *next;
        my_thread *prev;
        
        uint16_t id;
        thread_status status;

        pthread_t thread;

        func_ptr func;
        void *arg;

        pthread_mutex_t internl_mutex;
        pthread_cond_t start_signal;
    };

    struct my_thread_pool_s
    {
        my_thread *head;
        uint32_t size;

        uint32_t idle_count;
    };

    void *my_thread_update(void *arg);
    my_thread *pool_add_thread(my_thread_pool *pool);
    void pool_init(my_thread_pool **pool, uint32_t size);
    void pool_exec(my_thread_pool *pool, func_ptr func, void *args);
};

#endif