This tool demonstrates that CPUs can have different values for the same variable in the cache, at
the same time. 

* Compilation *
gcc -o multi_value_test -O3 multi_value.c -lpthread -Werror -Wall -D_GNU_SOURCE

* Plotter * 
The plotter can be run with python3 plotter.py. It requires matplotlib to be installed first.

* Discussion *
This should only be run on a multicore cpu and probably with hyperthreading disabled.