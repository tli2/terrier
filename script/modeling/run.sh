#########################################################################
# File Name: run.sh
# Author: Lin Ma
# mail: malin1993ml@gmail.com
# Created Time: 11/20/18
#########################################################################
#!/bin/bash

for METHOD in 'lr' 'kr' 'rf' 'nn'; do
    ./modeling.py --method $METHOD --mode raw --input_path \
        contention_benchmark_dedup_committed.csv --output_path results_committed.csv
done

