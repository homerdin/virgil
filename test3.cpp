#include <iostream>
#include <vector>
#include <math.h>

#include "ThreadPool.hpp"
#include "work.hpp"

int main (int argc, char *argv[]){

  /*
   * Fetch the inputs.
   */
  if (argc < 4){
    std::cerr << "USAGE: " << argv[0] << " THREADS TASKS ITERS_PER_TASK" << std::endl;
    return 1;
  }
  auto threads = atoi(argv[1]);
  auto tasks = atoi(argv[2]);
  auto iters = atoi(argv[3]);

  /*
   * Create a thread pool.
   */
  MARC::ThreadPool pool(threads);

  /*
   * Submit jobs.
   */
  for (auto i=0; i < tasks; i++){
    pool.submit(myF, iters);
  }

  return 0;
}
