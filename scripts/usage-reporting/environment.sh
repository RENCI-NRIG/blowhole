#!/bin/bash

SMLIST="uvanl rci ufl uh tamu fiu ucd osf sl ciena wsu bbn psc nicta gwu umass wvn uaf"
AMLIST="uvanl rci ufl uh tamu fiu ucd osf sl ciena wsu bbn psc nicta gwu umass wvn uaf"

declare -A exceptions
exceptions[uvanl]="uva-nl"

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

function findUrlShort {
	pattern=$1
	port=$2

	if [ "${exceptions[$pattern]}" != "" ]; then
		pattern=${exceptions[$pattern]}
	fi
	hName=$pattern-hn.exogeni.net
	RETURNURL=`grep $hName $PEQUODCONFIG | grep $port | cut -f1 -d','`
	if [ "$RETURNURL" = "" ]; then
       		echo "Unable to find URL for $pattern:$port"
        	exit 1
	fi
}
