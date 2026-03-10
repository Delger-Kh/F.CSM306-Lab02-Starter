#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <queue>

// ================================================================
// Serial
// ================================================================
class TaskSystemSerial : public ITaskSystem {
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

// ================================================================
// Parallel Spawn
// ================================================================
class TaskSystemParallelSpawn : public ITaskSystem {
public:
    int num_threads_;
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

// ================================================================
// Parallel Thread Pool Spinning
// ================================================================
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
    int num_threads_;
    std::vector<std::thread> thread_pool_;
    std::atomic<IRunnable*> runnable_;
    std::atomic<int> num_total_tasks_;
    std::atomic<int> next_task_;
    std::atomic<int> done_count_;
    std::atomic<bool> stop_;

    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

// ================================================================
// Parallel Thread Pool Sleeping — PART A ONLY, completely clean
// run() is blocking, no async logic whatsoever
// ================================================================
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
    int num_threads_;
    std::vector<std::thread> thread_pool_;
    std::mutex mtx_;
    std::condition_variable worker_cv_;
    std::condition_variable main_cv_;
    IRunnable* runnable_;
    int num_total_tasks_;
    int next_task_;
    std::atomic<int> done_count_;
    bool stop_;

    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    // Part A does NOT implement these — stubs only
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

// ================================================================
// TaskRecord — used only by Part B
// ================================================================
struct TaskRecord {
    TaskID id;
    IRunnable* runnable;
    int num_total_tasks;
    std::unordered_set<TaskID> pending_deps;
    int next_task;
    std::atomic<int> done_count;
    bool started;

    TaskRecord(TaskID id, IRunnable* r, int n, const std::vector<TaskID>& deps)
        : id(id), runnable(r), num_total_tasks(n),
          pending_deps(deps.begin(), deps.end()),
          next_task(0), done_count(0), started(false) {}
};

// ================================================================
// Parallel Thread Pool Sleeping — PART B
// Separate class, Part A sleeping is untouched
// Adds: runAsyncWithDeps(), sync() with full dep tracking
// ================================================================
class TaskSystemParallelThreadPoolSleepingPartB : public ITaskSystem {
public:
    int num_threads_;
    std::vector<std::thread> thread_pool_;
    std::mutex mtx_;
    std::condition_variable worker_cv_;
    std::condition_variable main_cv_;
    bool stop_;

    // Part B async state
    TaskID next_id_;
    std::unordered_map<TaskID, TaskRecord*> all_tasks_;
    std::queue<TaskRecord*> ready_queue_;
    int async_total_;
    int async_done_;

    TaskSystemParallelThreadPoolSleepingPartB(int num_threads);
    ~TaskSystemParallelThreadPoolSleepingPartB();
    const char *name();

    // Part B does NOT use run() — stub only
    void run(IRunnable *runnable, int num_total_tasks);

    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    void onTaskGroupFinished(TaskID finished_id);
};

#endif