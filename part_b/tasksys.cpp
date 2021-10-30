#include "tasksys.h"
#include <iostream>
#include <algorithm>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->current_taskid = 0;
    this->task_run_mutex = new std::mutex();
    this->finished_task_mutex = new std::mutex();
    this->task_run_cr = new std::condition_variable();
    this->finished_task_cr = new std::condition_variable();
    this->done_flag = false;
    // std::map<TaskID, std::set<TaskID>> tasksDep;
    for (int i = 0; i < num_threads; i++) {
        this->pool.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::runThreadParallelThreadPoolSleeping, this, i));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done_flag = true;
    task_run_cr->notify_all();
    for (int i = 0; i < num_threads; i++) {
        pool[i].join();
    }
    delete task_run_mutex;
    delete finished_task_mutex;
    delete task_run_cr;
    delete finished_task_cr;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
    return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
    if (deps.size() == 0) {
        tasks_dep[current_taskid];
    }
    for (auto dep: deps) {
        tasks_dep[current_taskid].insert(dep);
    }
    task_id_to_task[current_taskid] = new Task(current_taskid, runnable, num_total_tasks);
    return current_taskid++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    
    scanForReadyTasks();
    bool done_work = false;
    while (!done_work) {
        std::unique_lock<std::mutex> finished_task_lock(*finished_task_mutex);
        // put main thread to sleep when finished tasks are empty
        while (finished_tasks.empty()) {
            finished_task_cr->wait(finished_task_lock);
        }
        bool has_more_finished_tasks = true;
        while (has_more_finished_tasks) {
            // pop finished task from queue and remove it from dependencies graph
            auto task_done_id = finished_tasks.front();
            finished_tasks.pop_front();
            remaining_tasks.erase(remaining_tasks.find(task_done_id));
            finished_task_lock.unlock();
            removeTaskIDFromDependency(task_done_id);
            finished_task_lock.lock();
            has_more_finished_tasks = !finished_tasks.empty();
        }
        finished_task_lock.unlock();
        // scan for tasks with no dependencies and wake up thread worker
        scanForReadyTasks();
        finished_task_mutex->lock();
        done_work = tasks_dep.empty() && remaining_tasks.empty();
        finished_task_mutex->unlock();
    }
    return;
}

void TaskSystemParallelThreadPoolSleeping::runThreadParallelThreadPoolSleeping(int thread_number) {
    while (!done_flag) {
        bool done_work = false; 
        while (!done_work) {
            std::unique_lock<std::mutex> task_run_lock(*task_run_mutex);
            // sleep worker thread when there are no ready tasks
            while (runnable_tasks.empty() && !done_flag) {
                task_run_cr->wait(task_run_lock);
            }
            done_work = runnable_tasks.empty();
            if (done_work) {
                task_run_lock.unlock();
                continue;
            }
            // pop work and execute
            auto task = runnable_tasks.front();
            runnable_tasks.pop_front();
            task_run_lock.unlock();

            task->runnable->runTask(task->current_task, task->num_total_tasks);

            // decrement task counter and add it to finished queue when there are
            // no more work for this taskid
            finished_task_mutex->lock();
            remaining_tasks[task->id]--;
            if (remaining_tasks[task->id] <= 0) {
                finished_tasks.push_back(task->id);
                finished_task_mutex->unlock();
                finished_task_cr->notify_one();
            } else {
                finished_task_mutex->unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::scanForReadyTasks() {
   for (auto it = tasks_dep.begin(); it != tasks_dep.end();) { 
        auto tasks = it->second;
        if (tasks.empty()) {
            Task* t = task_id_to_task[it->first];
            remaining_tasks[t->id] = t->num_total_tasks;
            task_run_mutex->lock();
            for (int i = 0; i < t->num_total_tasks; i++) {
                runnable_tasks.push_back(new RunnableTask(t->id, i, t->runnable, t->num_total_tasks));
            }
            task_run_mutex->unlock();
            task_run_cr->notify_all();
            it = tasks_dep.erase(it);
       } else {
           ++it;
       }
   }
}

void TaskSystemParallelThreadPoolSleeping::removeTaskIDFromDependency(TaskID finished_task) {
   for (auto it = tasks_dep.begin(); it != tasks_dep.end();it++) { 
        it->second.erase(finished_task);
   } 
}

void TaskSystemParallelThreadPoolSleeping::printSet(std::set<TaskID> tasks) {
    for (auto task_it = tasks.begin(); task_it != tasks.end();task_it++) {
        std::cout << *task_it << ", ";
    }
}

void TaskSystemParallelThreadPoolSleeping::printTasksDep() {
   for (auto it = tasks_dep.begin(); it != tasks_dep.end();it++) { 
        auto tasks = it->second;
        std::cout << it->first << ": ";
        printSet(tasks);
        std::cout << std::endl;
   }  
   std::cout << "==========" << std::endl;
}

