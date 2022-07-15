/*
 * Copyright 2020 - 2021  Simone Campanoni
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *
 * The ThreadPoolForCMultiQueues class.
 * Keeps a set of threads constantly waiting to execute incoming jobs.
 */
#pragma once

#include "ThreadSafeMutexQueue.hpp"
#include "ThreadSafeSpinLockQueue.hpp"
#include "ThreadCTask.hpp"
#include "TaskFuture.hpp"
#include "ThreadPoolForC.hpp"

#include <assert.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <sched.h>
#include <sstream>
#include <string>

#define BRIAN 0
//#define BRIAN 1

typedef int LocalityIsland;

namespace MARC {

  /*
   * Thread pool.
   */
  class ThreadPoolForCMultiQueues : public ThreadPoolForC {
    public:

      /*
       * Default constructor.
       *
       * By default, the thread pool is not extendible and it creates at least one thread.
       */
      ThreadPoolForCMultiQueues(void);

      /*
       * Constructor.
       */
      explicit ThreadPoolForCMultiQueues (
        const bool extendible,
        const std::uint32_t numThreads = std::max(std::thread::hardware_concurrency(), 2u) - 1u,
        std::function <void (void)> codeToExecuteAtDeconstructor = nullptr
        );

      /*
       * Submit a job to be run by the thread pool and detach it from the caller.
       */
      void submitAndDetach (
        void (*f) (void *args),
        void *args
        ) override;

      /*
       * Submit a job to be run by the thread pool and detach it from the caller.
       */
      void submitAndDetach (
        void (*f) (void *args),
        void *args,
        LocalityIsland li
        );

      /*
       * Return the number of tasks that did not start executing yet.
       */
      std::uint64_t numberOfTasksWaitingToBeProcessed (void) const override ;

      /*
       * Destructor.
       */
      ~ThreadPoolForCMultiQueues(void);

      /*
       * Non-copyable.
       */
      ThreadPoolForCMultiQueues(const ThreadPoolForCMultiQueues& rhs) = delete;

      /*
       * Non-assignable.
       */
      ThreadPoolForCMultiQueues& operator=(const ThreadPoolForCMultiQueues& rhs) = delete;

    protected:

      /*
       * Object fields.
       */
//      std::vector<ThreadSafeSpinLockQueue<ThreadCTask *>*> cWorkQueues;
      std::vector<ThreadSafeMutexQueue<ThreadCTask *>*> cWorkQueues;
      mutable pthread_spinlock_t cWorkQueuesLock;

      /*
       * Constantly running function each thread uses to acquire work items from the queue.
       */
      void workerFunction (std::atomic_bool *availability, std::uint32_t thread) override ;
  };

}

MARC::ThreadPoolForCMultiQueues::ThreadPoolForCMultiQueues(void) 
  : ThreadPoolForCMultiQueues{false} 
  {

  /*
   * Always create at least one thread.  If hardware_concurrency() returns 0,
   * subtracting one would turn it to UINT_MAX, so get the maximum of
   * hardware_concurrency() and 2 before subtracting 1.
   */

  return ;
}

MARC::ThreadPoolForCMultiQueues::ThreadPoolForCMultiQueues (
  const bool extendible,
  const std::uint32_t numThreads,
  std::function <void (void)> codeToExecuteAtDeconstructor)
  :
      ThreadPoolForC{extendible, numThreads, codeToExecuteAtDeconstructor}
  {
  pthread_spin_init(&this->cWorkQueuesLock, 0);

//  std::cerr << "B: starting " << numThreads+1 << "Queues\n";
  /*
   * Create 1 queue per thread
   */
  for (auto i = 0; i < numThreads; i++){
//    cWorkQueues.push_back(new ThreadSafeSpinLockQueue<ThreadCTask *>);
    cWorkQueues.push_back(new ThreadSafeMutexQueue<ThreadCTask *>);
  }

  /*
   * Start threads.
   */
  try {
    this->newThreads(numThreads);

  } catch(...) {
    throw;
  }

  return ;
}

void MARC::ThreadPoolForCMultiQueues::submitAndDetach (
  void (*f) (void *args),
  void *args
  ){

  // locality 0 is used by main thread
  // the actual index is adjusted later
  static std::uint32_t nextLocality = 1;
  this->submitAndDetach(f, args, nextLocality);
  nextLocality++;
}

void MARC::ThreadPoolForCMultiQueues::submitAndDetach (
  void (*f) (void *args),
  void *args,
  LocalityIsland li
  ){

//  std::cerr << "Going to submit something\n";
  /*
   * Fetch the memory.
   */
  auto cTask = this->getTask();
  cTask->setFunction(f, args);

  /*
   * Submit the task.
   */
  if (this->extendible){
    std::cerr << "WTF: locking workQueuesLock\n";
    pthread_spin_lock(&this->cWorkQueuesLock);
  }

  auto adjustedLi = li-1;
  //auto adjustedLi = li;
  
  auto queueID = adjustedLi;// % this->cWorkQueues.size();
  if (false && adjustedLi > 110) {
    adjustedLi -= 111;
  }
  auto pq = getCurrentThreadQId();
  if(BRIAN) {
    std::stringstream msg;
    msg << "Submitting from " << pq << " to " << queueID << '\n';
    std::cerr << msg.str();
  }
  this->cWorkQueues.at(queueID)->push(cTask);

  if (this->extendible){
    pthread_spin_unlock(&this->cWorkQueuesLock);
  }

  /*
   * Expand the pool if possible and necessary.
   */
//  this->expandPool();

  return ;
}

void MARC::ThreadPoolForCMultiQueues::workerFunction (std::atomic_bool *availability, std::uint32_t thread){
    int  tp[] = {0,56,2,58,4,60,6,62,8,64,10,66,12,68,14,70,16,72,18,74,20,76,22,78,24,80,
                 26,82,28,84,30,86,32,88,34,90,36,92,38,94,40,96,42,98,44,100,46,102,48,104,
                 50,106,52,108,54,110,1,57,3,59,5,61,7,63,9,65,11,67,13,69,15,71,17,73,19,75,
                 21,77,23,79,25,81,27,83,29,85,31,87,33,89,35,91,37,93,39,95,41,97,43,99,45,101,
                 47,103,49,105,51,107,53,109,55,111};


//    int tp[] = {0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,
  //              1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,
    //            56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,94,96,98,100,102,104,106,108,110,
      //          57,59,61,63,65,67,69,71,73,75,77,79,81,83,85,87,89,91,93,95,97,99,101,103,105,107,109,111};

//    int tp[] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,
  //              29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
    //            57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,
      //          85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111};


  /*
   * Fetch the queue of the thread
   */
  auto self = pthread_self();
  auto cpu = tp[threadQId];//QIdToQCoreMap[thread+1];
  auto qID = threadQId-1;//coreToQIdMap[cpu];
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  if(BRIAN) {std::cout << "Worker started " << std::endl
            << "It's thread id is " << thread << std::endl
            << "It's logical id(affinity) is " << cpu << std::endl
            << "It's Qid is " << qID << std::endl
            << "It's threadQId is " << threadQId << std::endl;}

  
  pthread_setaffinity_np(self, sizeof(cpu_set_t), &cpuset);
  pthread_spin_lock(&this->cWorkQueuesLock);
  auto threadQueue = this->cWorkQueues.at(qID);
//            << "cWorkQueues Size = " << cWorkQueues.size() << std::endl
  //          << "threadQ = " << threadQueue << std::endl;
  pthread_spin_unlock(&this->cWorkQueuesLock);

  while(!m_done) {
    (*availability) = true;
    ThreadCTask *pTask = nullptr;
    if(threadQueue->waitPop(pTask)) {
//      std::cerr << "B: about to execute task\n";
      (*availability) = false;
      pTask->execute();
    }
    if (m_done) {
      break;
    }
    if (pTask){
      pTask->setAvailable();
    }
  }

  return ;
}

std::uint64_t MARC::ThreadPoolForCMultiQueues::numberOfTasksWaitingToBeProcessed (void) const {
  std::uint64_t s = 0;
  pthread_spin_lock(&this->cWorkQueuesLock);
  for (auto queue : this->cWorkQueues) {
    s += queue->size();
  }
  pthread_spin_unlock(&this->cWorkQueuesLock);

  return s;
}

MARC::ThreadPoolForCMultiQueues::~ThreadPoolForCMultiQueues (void){

  /*
   * Signal threads to quite.
   */
  this->m_done = true;
  pthread_spin_lock(&this->cWorkQueuesLock);
  for (auto queue : this->cWorkQueues) {
    queue->invalidate();
  }
  pthread_spin_unlock(&this->cWorkQueuesLock);

  /*
   * Wait for all threads to start or avoid to start.
   */
  this->waitAllThreadsToBeUnavailable();

  /*
   * Join threads.
   */
  return ;
}
