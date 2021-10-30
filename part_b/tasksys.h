#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <map>
#include <thread>
#include <tuple>
#include <deque>
#include <iostream>
#include <mutex>
#include <set>
#include <condition_variable>

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
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
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
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class Task {
   public:
        TaskID id;
        IRunnable* runnable;
        int num_total_tasks;

        Task(TaskID id, IRunnable* runnable, int num_total_tasks) {
            this->id = id;
            this->runnable = runnable;
            this->num_total_tasks = num_total_tasks;
        }
        Task(const Task &t) {
            this->id = t.id;
            this->runnable = t.runnable;
            this->num_total_tasks = t.num_total_tasks;
        }
        friend std::ostream& operator<< (std::ostream &out, Task const& task) {
            return out << "<id: " << task.id << ", total: " << task.num_total_tasks << ">" << std::endl;
        }
};

class RunnableTask {
   public:
        TaskID id;
        IRunnable* runnable;
        int current_task;
        int num_total_tasks;

        RunnableTask(TaskID id, TaskID current_task, IRunnable* runnable, int num_total_tasks) {
            this->id = id;
            this->runnable = runnable;
            this->current_task = current_task;
            this->num_total_tasks = num_total_tasks;
        }
        RunnableTask(const RunnableTask &t) {
            this->id = t.id;
            this->runnable = t.runnable;
            this->current_task = t.current_task;
            this->num_total_tasks = t.num_total_tasks;
        }
        friend std::ostream& operator<< (std::ostream &out, RunnableTask const& task) {
            return out << "<id: " << task.id << ", current: " << task.current_task << ", total: " << task.num_total_tasks << ">" << std::endl;
        }
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        bool done_flag;
        int num_threads;
        int current_taskid;
        // task dependencies
        std::map<TaskID, std::set<TaskID>> tasks_dep;
        // map from taskid -> task detail
        std::map<TaskID, Task*> task_id_to_task;
        // map to track how many tasks left for each task id
        std::map<TaskID, int> remaining_tasks;
        // queue for ready tasks
        std::deque<RunnableTask*> runnable_tasks;
        // queue for finished tasks to be removed from dependencies
        std::deque<TaskID> finished_tasks;
        std::vector<std::thread> pool;
        std::mutex* task_run_mutex; 
        std::mutex* finished_task_mutex;
        std::condition_variable* task_run_cr;
        std::condition_variable* finished_task_cr;

        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void runThreadParallelThreadPoolSleeping(int thread_number);
        void scanForReadyTasks();
        void removeTaskIDFromDependency(TaskID i);
        void printTasksDep();
        void printSet(std::set<TaskID> tasks);
};

#endif
