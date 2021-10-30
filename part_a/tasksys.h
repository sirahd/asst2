#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

class ThreadState {
    public:
        std::condition_variable* main_cr;
        std::condition_variable* task_done_cr;
        std::mutex* main_mutex;
        std::mutex* task_done_mutex;
        int current_task;
        int task_done;
        bool done_flag;
        IRunnable* runnable;
        int num_total_tasks;
        ThreadState() {
            main_cr = new std::condition_variable();
            task_done_cr = new std::condition_variable();
            main_mutex = new std::mutex();
            task_done_mutex = new std::mutex();
            done_flag = false;
            current_task = 0;
            num_total_tasks = 0;
            task_done = 0;
        }
        ~ThreadState() {
            delete main_cr;
            delete main_mutex;
            delete task_done_mutex;
            delete task_done_cr;
        }
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        int num_threads;
        ThreadState* thread_state;
        std::vector<std::thread> pool;

        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        void runThreadParallelSpawn(IRunnable* runnable, std::atomic_int& current_task, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        int num_threads;
        ThreadState* thread_state;
        std::vector<std::thread> pool;

        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void runThreadParallelThreadPoolSpinning(int thread_number);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        int num_threads;
        ThreadState* thread_state;
        std::vector<std::thread> pool;

        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void runThreadParallelThreadPoolSleeping(int thread_number);
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

#endif
