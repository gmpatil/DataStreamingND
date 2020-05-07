#!/bin/bash
declare -a packages=( 
"findspark"
"pyspark"
"pathlib"
"kafka"
"confluent_kafka"
"fastavro"
"dataclasses"
)

printf "\nFinding Python package versions if installed.\n" 
for p in ${packages[@]}; do
	v=$(python -c "import $p; print($p.__version__)" 2>&1 )
	if [ $? == 1 ]
	then
		echo "$p - Not installed."
	else
		echo "$p - $v"
	fi
done

# Finding Python package versions if installed.
# findspark - 1.3.0
# pyspark - 2.3.4
# pathlib - Not installed.
# kafka - 1.4.7
# confluent_kafka - 1.1.0
# fastavro - 0.22.7
# dataclasses - Not installed



