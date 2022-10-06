#include "my_thread_pool.h"

extern "C"
{
    uint32_t thread_id_counter = 0;
    pthread_mutex_t pool_mutex = PTHREAD_MUTEX_INITIALIZER;

    void *my_thread_update(void *arg)
    {
        my_thread *thread_info = (my_thread *)arg;

        loop_start:
        pthread_mutex_lock(&thread_info->internl_mutex);
        
        while (thread_info->status != RUNNING)
            pthread_cond_wait(&thread_info->start_signal, &thread_info->internl_mutex);

        pthread_mutex_unlock(&thread_info->internl_mutex);
        
        if (thread_info->func && thread_info->arg)
        {
            while (thread_info->status != STOPPED)
            {
                func_ptr f = thread_info->func;
                void *r = f(thread_info->arg);
                if (!r)
                {    
                    thread_info->status = IDLE;
                    pthread_mutex_lock(&pool_mutex);
                    thread_info->pool->idle_count++;
                    pthread_mutex_unlock(&pool_mutex);
                    goto loop_start;
                }
                sleep(60);
            }
        }

        return (void *)NULL;
    }

    my_thread *pool_add_thread(my_thread_pool *pool)
    {
        my_thread *thread = (my_thread *)calloc(1, sizeof(my_thread));
        thread->id = thread_id_counter++;
        thread->status = IDLE;
        thread->pool = pool;

        pthread_mutex_init(&thread->internl_mutex, 0);
        pthread_cond_init(&thread->start_signal, 0);

        if (pool->head)
        {
            thread->next = pool->head;
            thread->prev = pool->head->prev;
            pool->head->prev->next = thread;
            pool->head->prev = thread;
        }
        else
        {
            pool->head = thread;
            thread->next = thread;
            thread->prev = thread;
        }
        
        pthread_create(&thread->thread, NULL, my_thread_update, thread);
        return thread;
    }

    void pool_init(my_thread_pool **pool, uint32_t size)
    {
        *pool = (my_thread_pool *)calloc(1, sizeof(my_thread_pool));
        (*pool)->size = size;
        (*pool)->idle_count = size;

        for (size_t i = 0; i < size; i++)
            pool_add_thread(*pool);
    }

    void pool_exec(my_thread_pool *pool, func_ptr func, void *arg)
    {
        pthread_mutex_lock(&pool_mutex);

        my_thread *idle = NULL;
        if (pool->idle_count == 0)
            idle = pool_add_thread(pool);
        else
        {
            my_thread *ptr = pool->head;
            do
            {
                if (ptr->status == IDLE)
                {
                    idle = ptr;
                    break;
                }
                ptr = ptr->next;

            } while (ptr != pool->head);
            pool->idle_count--;
        }

        if (idle)
        {
            idle->func = func;
            idle->arg = arg;
            idle->status = RUNNING;
            pthread_mutex_lock(&idle->internl_mutex);
            pthread_cond_signal(&idle->start_signal);
            pthread_mutex_unlock(&idle->internl_mutex);
        }

        pthread_mutex_unlock(&pool_mutex);
    }
};