#!/bin/bash

SMLIST="rci ufl uh tamu fiu ucd osf sl ciena wsu bbn psc nicta gwu umass wvn uaf"
AMLIST="rci ufl uh tamu fiu ucd osf sl ciena wsu bbn psc nicta gwu umass wvn uaf"

PEQUOD=/home/ibaldin/bin/bin/pequod
PEQUODCONFIG="/home/ibaldin/.pequod/properties"

function findUrl {
	pattern=$1
	port=$2

	RETURNURL=`grep $pattern $PEQUODCONFIG | grep $port | cut -f1 -d','`
	if [ "$RETURNURL" = "" ]; then
       		echo "Unable to find URL for $pattern:$port"
        	exit 1
	fi
}
