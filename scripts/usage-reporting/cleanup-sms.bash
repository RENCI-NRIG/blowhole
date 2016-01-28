#!/bin/bash

. ./environment.sh

# Exosm
cat > cleanup-scripts/exo-sm.pq << end-of-file
show deadslices for exo-sm
set current slices
show reservations for current actor exo-sm 
set current reservations
manage remove slice current actor exo-sm
manage remove reservation current actor exo-sm
manage remove slice current actor ndl-broker
manage remove reservation current actor ndl-broker
end-of-file

findUrl geni.renci.org 12443
URL1=$RETURNURL
findUrl geni.renci.org 14443
URL2=$RETURNURL
echo Cleaning up Exo-SM on $URL1,$URL2
$PEQUOD -f cleanup-scripts/exo-sm.pq -u $URL1,$URL2

for SM in $SMLIST; do
	cat > cleanup-scripts/$SM-sm.pq << end-of-file
show deadslices for $SM-sm
set current slices
show reservations for current actor $SM-sm
set current reservations
manage remove slice current actor $SM-sm
manage remove reservation current actor $SM-sm
manage remove slice current actor $SM-broker
manage remove reservation current actor $SM-broker
end-of-file
	findUrlShort $SM 12443
	URL1=$RETURNURL
	findUrlShort $SM 14443
	URL2=$RETURNURL
	echo Cleaning up $SM-sm on $URL1,$URL2
	$PEQUOD -f cleanup-scripts/$SM-sm.pq -u $URL1,$URL2
done

