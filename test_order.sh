#!/bin/bash

for i in {1..100}
do
    ./cl put 5000 USER $i &
done

wait

./cl get 5000 USER