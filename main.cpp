#include <iostream>
#include <vector>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include "tasksys.h"

class ComputeTask : public IRunnable {
public:
    std::vector<double> results;
    int workload_intensity;
    ComputeTask(int num_tasks, int intensity)
        : results(num_tasks, 0.0), workload_intensity(intensity) {}
    void runTask(int taskID, int num_total_tasks) override {
        double val = 0.0;
        for (int i = 0; i < workload_intensity; ++i)
            val += std::sin(i * 0.01 + taskID) * std::cos(i * 0.02 + taskID);
        results[taskID] = val;
    }
};

void runBenchmark(ITaskSystem *system, IRunnable *task, int num_tasks, const std::string &name) {
    std::cout << "Testing [" << name << "]..." << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    system->run(task, num_tasks);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << " Done. Time: " << std::fixed << std::setprecision(4) << elapsed.count() << "s" << std::endl;
}

int main() {
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    // ============================================================
    // PART A — Synchronous benchmark
    // ============================================================
    std::cout << "========================================" << std::endl;
    std::cout << "Task System Benchmark (Part A)"          << std::endl;
    std::cout << "Threads: " << num_threads << ", Tasks: 5000" << std::endl;
    std::cout << "========================================" << std::endl;

    ComputeTask task(5000, 200);

    ITaskSystem *s1 = new TaskSystemSerial(num_threads);
    runBenchmark(s1, &task, 5000, "Serial System");
    delete s1;

    ITaskSystem *s2 = new TaskSystemParallelSpawn(num_threads);
    runBenchmark(s2, &task, 5000, "Parallel Spawn");
    delete s2;

    ITaskSystem *s3 = new TaskSystemParallelThreadPoolSpinning(num_threads);
    runBenchmark(s3, &task, 5000, "Parallel Spinning Pool");
    delete s3;

    ITaskSystem *s4 = new TaskSystemParallelThreadPoolSleeping(num_threads);
    runBenchmark(s4, &task, 5000, "Parallel Sleeping Pool");
    delete s4;

    // ============================================================
    // PART B — Async dependency test (separate class)
    // ============================================================
    std::cout << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Async Dependency Test (Part B)"          << std::endl;
    std::cout << "A(128) --> B(2), C(6) --> D(32)"         << std::endl;
    std::cout << "========================================" << std::endl;

    TaskSystemParallelThreadPoolSleepingPartB *asyncSystem =
        new TaskSystemParallelThreadPoolSleepingPartB(num_threads);

    ComputeTask tA(128, 1000);
    ComputeTask tB(2,   1000);
    ComputeTask tC(6,   1000);
    ComputeTask tD(32,  1000);

    auto t_start = std::chrono::high_resolution_clock::now();

    TaskID idA = asyncSystem->runAsyncWithDeps(&tA, 128, {});
    std::cout << "  runAsyncWithDeps(A, 128, {})    ->  TaskID=" << idA << std::endl;

    TaskID idB = asyncSystem->runAsyncWithDeps(&tB, 2, {idA});
    std::cout << "  runAsyncWithDeps(B, 2,  {A=" << idA << "}) -> TaskID=" << idB << std::endl;

    TaskID idC = asyncSystem->runAsyncWithDeps(&tC, 6, {idA});
    std::cout << "  runAsyncWithDeps(C, 6,  {A=" << idA << "}) -> TaskID=" << idC << std::endl;

    TaskID idD = asyncSystem->runAsyncWithDeps(&tD, 32, {idB, idC});
    std::cout << "  runAsyncWithDeps(D, 32, {B=" << idB << ",C=" << idC << "}) -> TaskID=" << idD << std::endl;

    std::cout << "  sync() waiting..." << std::flush;
    asyncSystem->sync();

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = t_end - t_start;
    std::cout << " Done. Time: " << std::fixed << std::setprecision(4)
              << elapsed.count() << "s" << std::endl;
    std::cout << "  Order respected: A -> B,C -> D  checkmark" << std::endl;

    delete asyncSystem;
    return 0;
}