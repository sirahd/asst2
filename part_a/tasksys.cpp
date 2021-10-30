#include "tasksys.h"
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::runThreadParallelSpawn(IRunnable* runnable, std::atomic_int& current_task, int num_total_tasks) {
    int i;
    while ((i = current_task++) < num_total_tasks) {
        runnable->runTask(i, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<std::thread> threads;
    std::atomic_int current_task(0);

    for (int i = 0; i < this->num_threads; i++) {
        threads.push_back(std::thread(&TaskSystemParallelSpawn::runThreadParallelSpawn, this, runnable, std::ref(current_task), num_total_tasks));
    }
    for (int i = 0; i < this->num_threads; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */



const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::runThreadParallelThreadPoolSpinning(int thread_number) {
    while (!thread_state->done_flag) {
        thread_state->main_mutex->lock();
        int i = thread_state->current_task;
        int total_task = thread_state->num_total_tasks;
        if (total_task == 0 || i >= total_task) {
            thread_state->main_mutex->unlock();
            continue;
        }
        thread_state->current_task++;
        thread_state->main_mutex->unlock();
        thread_state->runnable->runTask(i, total_task); 
        thread_state->task_done_mutex->lock();
        thread_state->task_done++;
        thread_state->task_done_mutex->unlock();
     }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->thread_state = new ThreadState();
    this->num_threads = num_threads;
    for (int i = 0; i < num_threads; i++) {
        this->pool.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::runThreadParallelThreadPoolSpinning, this, i));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->thread_state->done_flag = true;
    for (int i = 0; i < this->num_threads; i++) {
        this->pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // std::cout << "start\n";
    // initialize thread state to the new run
    thread_state->main_mutex->lock();
    thread_state->current_task = 0;
    thread_state->runnable = runnable;
    thread_state->num_total_tasks = num_total_tasks;
    thread_state->done_flag = false;
    thread_state->task_done = 0;
    thread_state->main_mutex->unlock();
    bool done_work = false;
    // loop to check if all works are done
    while (!done_work) {
        thread_state->task_done_mutex->lock();
        done_work = thread_state->task_done >= thread_state->num_total_tasks;
        thread_state->task_done_mutex->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::runThreadParallelThreadPoolSleeping(int thread_number) {
    while (!thread_state->done_flag) {
        std::unique_lock<std::mutex> main_lock(*thread_state->main_mutex);
        // sleep thread worker when there is no work to execute
        while ((thread_state->num_total_tasks == 0 || thread_state->current_task >= thread_state->num_total_tasks) && 
                !thread_state->done_flag) {
            thread_state->main_cr->wait(main_lock);
        }
        int i = thread_state->current_task;
        int total_task = thread_state->num_total_tasks;
        // keep looping if current task is done
        if (total_task == 0 || i >= total_task) {
            main_lock.unlock();
            continue;
        }
        thread_state->current_task++;
        main_lock.unlock();
        thread_state->runnable->runTask(i, total_task); 
        // increment task done counter
        std::unique_lock<std::mutex> task_done_lock(*thread_state->task_done_mutex);
        thread_state->task_done++;
        task_done_lock.unlock();
        // have one thread designated as the waker or wake when tasks are finished
        if (thread_number == 0 || thread_state->task_done >= thread_state->num_total_tasks) {
            thread_state->task_done_cr->notify_one();
        }
     } 
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->thread_state = new ThreadState();
    this->num_threads = num_threads;
    for (int i = 0; i < num_threads; i++) {
        pool.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::runThreadParallelThreadPoolSleeping, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    thread_state->done_flag = true;
    thread_state->main_cr->notify_all();
    for (int i = 0; i < this->num_threads; i++) {
        pool[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    thread_state->main_mutex->lock();
    thread_state->current_task = 0;
    thread_state->runnable = runnable;
    thread_state->num_total_tasks = num_total_tasks;
    thread_state->done_flag = false;
    thread_state->task_done = 0;
    thread_state->main_mutex->unlock();
    thread_state->main_cr->notify_all();
    bool done_work = false;
    // loop to check if all works are done
    while (!done_work) {
        std::unique_lock<std::mutex> task_done_lock(*thread_state->task_done_mutex); 
        if (thread_state->task_done < thread_state->num_total_tasks) {
            thread_state->main_cr->notify_all();
        }
        // sleep main thread when all tasks are not done
        while (thread_state->task_done < thread_state->num_total_tasks) {
            thread_state->task_done_cr->wait(task_done_lock);
        }
        done_work = thread_state->task_done >= thread_state->num_total_tasks;
        task_done_lock.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
