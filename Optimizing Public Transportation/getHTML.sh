#!/bin/bash
pushd .
cd /home/workspace/ 
while true
do 
    curl http://localhost:8889 > /home/workspace/html/cta.html 
    printf "Refreshed.\n" 
    sleep 10
    
    if (disaster-condition)
    then
    break    #Abandon the loop.
    fi    
done

popd