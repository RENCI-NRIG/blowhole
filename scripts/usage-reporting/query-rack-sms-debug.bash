#!/bin/bash +x

. ./environment.sh

PEQUOD=/home/ibaldin/bin/bin/pequod
SCRIPTDIR="scripts"
PEQUODCONFIG="/home/ibaldin/.pequod/properties"

# Special handling for ExoSM
SM=exo
findUrl geni.renci.org 14080
URL=$RETURNURL

SCRIPTNAME=$SCRIPTDIR/$SM-ActiveCheck.pq
# generate file
cat > $SCRIPTNAME << end-of-file
show reservations for all actor $SM-sm state active filter "e.vlan"
show reservations for all actor $SM-sm state active filter "Net.vlan"
show reservations for all actor $SM-sm state active filter "ion.vlan"
show reservations for all actor $SM-sm state active filter "nlr.vlan"
show reservations for all actor $SM-sm state active filter "e.vm"
show reservations for all actor $SM-sm state active filter "baremetal"
show reservations for all actor $SM-sm state active filter "lun"
end-of-file
# query
SMCOUNTS=`$PEQUOD -f $SCRIPTNAME -u $URL | grep Total | cut -f2 -d ' '`

#echo $SM [$SMCOUNTS]
# parse the results
LOCAL_VLAN=`echo $SMCOUNTS | cut -f1 -d' '`
TRANSIT_VLAN=`echo $SMCOUNTS | cut -f2 -d' '`
GLOBAL_VLAN=`echo $SMCOUNTS | cut -f3 -d' '`
MP_VLAN=`echo $SMCOUNTS | cut -f4 -d' '`
VM=`echo $SMCOUNTS | cut -f5 -d' '`
BM=`echo $SMCOUNTS | cut -f6 -d' '`
LUN=`echo $SMCOUNTS | cut -f7 -d' '`

# insert in DB
rrdtool updatev rrd/$SM-sm.rrd -t local_vlan:transit_vlan:global_vlan:mp_vlan:vm:bm:lun N:$LOCAL_VLAN:$TRANSIT_VLAN:$GLOBAL_VLAN:$MP_VLAN:$VM:$BM:$LUN

# the rest of SMs
for SM in $SMLIST; do 
  # find URL
  findUrl $SM-hn.exogeni.net 14080
  URL=$RETURNURL
  SCRIPTNAME=$SCRIPTDIR/$SM-ActiveCheck.pq
  # generate file
  cat > $SCRIPTNAME << end-of-file
show reservations for all actor $SM-sm state active filter "e.vlan"
show reservations for all actor $SM-sm state active filter "Net.vlan"
show reservations for all actor $SM-sm state active filter "e.vm"
show reservations for all actor $SM-sm state active filter "baremetal"
show reservations for all actor $SM-sm state active filter "lun"
end-of-file
  # query
  SMCOUNTS=`$PEQUOD -f $SCRIPTNAME -u $URL | grep Total | cut -f2 -d ' '`
  #echo $SM [$SMCOUNTS]
  LOCAL_VLAN=`echo $SMCOUNTS | cut -f1 -d' '`
  TRANSIT_VLAN=`echo $SMCOUNTS | cut -f2 -d' '`
  VM=`echo $SMCOUNTS | cut -f3 -d' '`
  BM=`echo $SMCOUNTS | cut -f4 -d' '`
  LUN=`echo $SMCOUNTS | cut -f5 -d' '`
  rrdtool updatev rrd/$SM-sm.rrd -t local_vlan:transit_vlan:vm:bm:lun N:$LOCAL_VLAN:$TRANSIT_VLAN:$VM:$BM:$LUN
done
