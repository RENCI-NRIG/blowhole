#!/bin/bash +x

. ./environment.sh

SCRIPTDIR="scripts"
GRAPHDIR="graphs"

DURATIONS="86400 604800 2419200 31536000"
GRAPHSIZE="-w 800 -h 400"
VMCOLOR="#0000FF"
BMCOLOR="#00FF00"
LUNCOLOR="#00FFFF"
GLCOLOR="#FF0000"
TLCOLOR="#FFAA00"
LLCOLOR="#FF00AA"

# Special handling for ExoSM
SM=exo
RRD=rrd/$SM-sm.rrd

# Graph the results for different durations
for duration in $DURATIONS; do
	GRAPHNAME=$GRAPHDIR/$SM-sm-$duration.png
	rrdtool graph $GRAPHNAME -a PNG $GRAPHSIZE --start -$duration --end now DEF:VM=$RRD:vm:MAX LINE1:VM$VMCOLOR:"VMs" DEF:BM=$RRD:bm:MAX LINE1:BM$BMCOLOR:"BareMetal nodes" DEF:LUN=$RRD:lun:MAX LINE1:LUN$LUNCOLOR:"Storage LUNs" DEF:TL=$RRD:transit_vlan:MAX LINE1:TL$TLCOLOR:"Transit LANs" DEF:LL=$RRD:local_vlan:MAX LINE1:LL$LLCOLOR:"Local LANs" DEF:GL=$RRD:global_vlan:MAX LINE1:GL$GLCOLOR:"Global LANs" >/dev/null
done


# the rest of SMs
for SM in $SMLIST; do 
	RRD=rrd/$SM-sm.rrd
	for duration in $DURATIONS; do
  		GRAPHNAME=$GRAPHDIR/$SM-sm-$duration.png
		rrdtool graph $GRAPHNAME -a PNG $GRAPHSIZE --start -$duration --end now DEF:VM=$RRD:vm:MAX LINE1:VM$VMCOLOR:"VMs" DEF:BM=$RRD:bm:MAX LINE1:BM$BMCOLOR:"BareMetal nodes" DEF:LUN=$RRD:lun:MAX LINE1:LUN$LUNCOLOR:"Storage LUNs" DEF:LL=$RRD:local_vlan:MAX LINE1:LL$LLCOLOR:"Local LANs"  >/dev/null
	done
done

# the rest of AMs
for AM in $AMLIST; do 
	RRD=rrd/$AM-am.rrd
	for duration in $DURATIONS; do
  		GRAPHNAME=$GRAPHDIR/$AM-am-$duration.png
		rrdtool graph $GRAPHNAME -a PNG $GRAPHSIZE --start -$duration --end now DEF:VM=$RRD:vm:MAX LINE1:VM$VMCOLOR:"VMs" DEF:BM=$RRD:bm:MAX LINE1:BM$BMCOLOR:"BareMetal nodes" DEF:LUN=$RRD:lun:MAX LINE1:LUN$LUNCOLOR:"Storage LUNs" DEF:LL=$RRD:local_vlan:MAX LINE1:LL$LLCOLOR:"Local LANs"  >/dev/null
	done
done
