#!/bin/bash
if [ $# -gt 0 ]
  then
    echo "Usage: bash mongo_health_check.sh"
    exit
fi
iw_home=$(echo $IW_HOME)
if [[ -z "$iw_home" ]]
    then
    echo "Please source $IW_HOME/bin/env.sh before running this script"
    exit
fi

version=$(python -V 2>&1 | grep -Po '(?<=Python )(.+)')
if [[ -z "$version" ]]
then
    echo "No Python!" 
else 
	parsedVersion=$(echo "${version//./}")
	if [[ "$parsedVersion" -lt "300" ]]
	then 
    	echo "Execute python2x"
    	python mongo_health_check_2x.py
	else
    	python mongo_health_check_3x.py  
	fi
fi