#include "tasksys.h"
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <queue>

IRunnable::~IRunnable() {}
ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

// ================================================================
// Serial
// ================================================================
const char *TaskSystemSerial::name() { return "Serial"; }
TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {}
TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++)
        runnable->runTask(i, num_total_tasks);
}
TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps) { return 0; }
void TaskSystemSerial::sync() { return; }


// ================================================================
// Parallel Spawn
// ================================================================
const char *TaskSystemParallelSpawn::name() { return "Parallel + Always Spawn"; }
TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads) {}
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
    std::vector<std::thread> threads;
    std::atomic<int> next_task(0);
    for (int t = 0; t < num_threads_; t++) {
        threads.push_back(std::thread([&]() {
            while (true) {
                int task_id = next_task.fetch_add(1);
                if (task_id >= num_total_tasks) break;
                runnable->runTask(task_id, num_total_tasks);
            }
        }));
    }
    for (auto &th : threads) th.join();
}
TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps) { return 0; }
void TaskSystemParallelSpawn::sync() { return; }


// ================================================================
// Parallel Thread Pool Spinning
// ================================================================
const char *TaskSystemParallelThreadPoolSpinning::name() { return "Parallel + Thread Pool + Spin"; }
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads),
      runnable_(nullptr), num_total_tasks_(0),
      next_task_(0), done_count_(0), stop_(false)
{
    for (int t = 0; t < num_threads_; t++) {
        thread_pool_.push_back(std::thread([this]() {
            while (true) {
                if (stop_.load()) return;
                if (runnable_.load() == nullptr) { std::this_thread::yield(); continue; }
                int task_id = next_task_.fetch_add(1);
                int total = num_total_tasks_.load();
                if (task_id < total) {
                    IRunnable* r = runnable_.load();
                    if (r) r->runTask(task_id, total);
                    if (done_count_.fetch_add(1) + 1 == total)
                        runnable_.store(nullptr);
                } else {
                    std::this_thread::yield();
                }
            }
        }));
    }
}
TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop_.store(true);
    for (auto &th : thread_pool_) th.join();
}
void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks) {
    next_task_.store(0); done_count_.store(0);
    num_total_tasks_.store(num_total_tasks);
    runnable_.store(runnable);
    while (runnable_.load() != nullptr) std::this_thread::yield();
}
TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) { return 0; }
void TaskSystemParallelThreadPoolSpinning::sync() { return; }


// ================================================================
// PART A — Parallel Thread Pool Sleeping
// 100% clean — zero async logic, zero Part B code
// ================================================================
const char *TaskSystemParallelThreadPoolSleeping::name() { return "Parallel + Thread Pool + Sleep"; }

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads),
      runnable_(nullptr),
      num_total_tasks_(0),
      next_task_(0),
      done_count_(0),
      stop_(false)
{
    for (int t = 0; t < num_threads_; t++) {
        thread_pool_.push_back(std::thread([this]() {
            while (true) {
                int task_id;
                int total;
                IRunnable* r;
                {
                    std::unique_lock<std::mutex> lock(mtx_);
                    // Only wakes up for: stop, or a sync run() job
                    worker_cv_.wait(lock, [this]() {
                        return stop_
                            || (runnable_ != nullptr && next_task_ < num_total_tasks_);
                    });
                    if (stop_) return;
                    task_id = next_task_++;
                    total   = num_total_tasks_;
                    r       = runnable_;
                } // mutex released — all threads work in parallel

                if (task_id < total && r != nullptr) {
                    r->runTask(task_id, total);
                }

                if (done_count_.fetch_add(1) + 1 == total) {
                    main_cv_.notify_one(); // wake main thread
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        stop_ = true;
    }
    worker_cv_.notify_all();
    for (auto &th : thread_pool_) th.join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks) {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        runnable_        = runnable;
        num_total_tasks_ = num_total_tasks;
        next_task_       = 0;
        done_count_.store(0);
    }
    worker_cv_.notify_all();

    std::unique_lock<std::mutex> lock(mtx_);
    main_cv_.wait(lock, [this]() {
        return done_count_.load() == num_total_tasks_;
    });
    runnable_ = nullptr;
}

// Part A stubs — not implemented
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) { return 0; }
void TaskSystemParallelThreadPoolSleeping::sync() { return; }


// ================================================================
// PART B — TaskSystemParallelThreadPoolSleepingPartB
// Completely separate class — does NOT touch Part A at all
// Only implements runAsyncWithDeps() + sync()
// ================================================================
const char *TaskSystemParallelThreadPoolSleepingPartB::name() {
    return "Parallel + Thread Pool + Sleep (Async)";
}

TaskSystemParallelThreadPoolSleepingPartB::TaskSystemParallelThreadPoolSleepingPartB(int num_threads)
    : ITaskSystem(num_threads),
      num_threads_(num_threads),
      stop_(false),
      next_id_(0),
      async_total_(0),
      async_done_(0)
{
    for (int t = 0; t < num_threads_; t++) {
        thread_pool_.push_back(std::thread([this]() {
            while (true) {
                TaskRecord* rec = nullptr;
                int task_id = -1;
                int total   = 0;

                {
                    std::unique_lock<std::mutex> lock(mtx_);
                    // Sleep until: stop, or a ready task exists
                    worker_cv_.wait(lock, [this]() {
                        return stop_ || !ready_queue_.empty();
                    });
                    if (stop_) return;
                    if (ready_queue_.empty()) continue;

                    rec = ready_queue_.front();

                    if (rec->next_task < rec->num_total_tasks) {
                        task_id = rec->next_task++;
                        total   = rec->num_total_tasks;
                        // If more tasks remain, wake another worker
                        if (rec->next_task < rec->num_total_tasks)
                            worker_cv_.notify_one();
                        else
                            ready_queue_.pop(); // last task taken, remove from queue
                    } else {
                        ready_queue_.pop();
                        continue;
                    }
                } // mutex released

                // Do the actual work — no lock held
                if (task_id >= 0 && rec != nullptr) {
                    rec->runnable->runTask(task_id, total);

                    // If this was the last task in the group, trigger dep resolution
                    if (rec->done_count.fetch_add(1) + 1 == total) {
                        std::unique_lock<std::mutex> lock(mtx_);
                        onTaskGroupFinished(rec->id);
                    }
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSleepingPartB::~TaskSystemParallelThreadPoolSleepingPartB() {
    {
        std::unique_lock<std::mutex> lock(mtx_);
        stop_ = true;
    }
    worker_cv_.notify_all();
    for (auto &th : thread_pool_) th.join();
    for (auto& kv : all_tasks_) delete kv.second;
}

// Part B does not use run() — stub
void TaskSystemParallelThreadPoolSleepingPartB::run(IRunnable *runnable, int num_total_tasks) {
    // Not used in Part B
}

TaskID TaskSystemParallelThreadPoolSleepingPartB::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps)
{
    std::unique_lock<std::mutex> lock(mtx_);

    TaskID new_id = next_id_++;
    TaskRecord* rec = new TaskRecord(new_id, runnable, num_total_tasks, deps);
    all_tasks_[new_id] = rec;
    async_total_++;

    // Remove deps that are already finished
    for (auto it = rec->pending_deps.begin(); it != rec->pending_deps.end(); ) {
        auto found = all_tasks_.find(*it);
        if (found != all_tasks_.end() &&
            found->second->done_count.load() == found->second->num_total_tasks)
            it = rec->pending_deps.erase(it);
        else
            ++it;
    }

    // No deps remaining → ready immediately
    if (rec->pending_deps.empty()) {
        rec->started = true;
        ready_queue_.push(rec);
        worker_cv_.notify_all();
    }

    return new_id; // non-blocking — returns immediately
}

void TaskSystemParallelThreadPoolSleepingPartB::onTaskGroupFinished(TaskID finished_id) {
    for (auto& kv : all_tasks_) {
        TaskRecord* rec = kv.second;
        if (rec->pending_deps.count(finished_id)) {
            rec->pending_deps.erase(finished_id);
            if (rec->pending_deps.empty() && !rec->started) {
                rec->started = true;
                ready_queue_.push(rec);
                worker_cv_.notify_all();
            }
        }
    }
    async_done_++;
    if (async_done_ == async_total_)
        main_cv_.notify_one(); // wake sync()
}

void TaskSystemParallelThreadPoolSleepingPartB::sync() {
    std::unique_lock<std::mutex> lock(mtx_);
    main_cv_.wait(lock, [this]() {
        return async_done_ == async_total_;
    });
    for (auto& kv : all_tasks_) delete kv.second;
    all_tasks_.clear();
    async_total_ = 0;
    async_done_  = 0;
}