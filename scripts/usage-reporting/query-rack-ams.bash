#!/bin/bash +x

. ./environment.sh

SCRIPTDIR="scripts"

echo $DATE > last_result_ams
# AMs
for AM in $AMLIST; do 
  # find URL
  findUrl $AM-hn.exogeni.net 12080
  URL=$RETURNURL
  SCRIPTNAME=$SCRIPTDIR/$AM-am-ActiveCheck.pq
  # generate file
  cat > $SCRIPTNAME << end-of-file
show reservations for all actor $AM-vm-am state active filter "e.vlan"
show reservations for all actor $AM-net-am state active filter "Net.vlan"
show reservations for all actor $AM-vm-am state active filter "e.vm"
show reservations for all actor $AM-vm-am state active filter "baremetal"
show reservations for all actor $AM-vm-am state active filter "lun"
end-of-file
  # query
  AMCOUNTS=`$PEQUOD -f $SCRIPTNAME -u $URL | grep Total | cut -f2 -d ' '`
  echo $AM [$AMCOUNTS]
  LOCAL_VLAN=`echo $AMCOUNTS | cut -f1 -d' '`
  TRANSIT_VLAN=`echo $AMCOUNTS | cut -f2 -d' '`
  VM=`echo $AMCOUNTS | cut -f3 -d' '`
  BM=`echo $AMCOUNTS | cut -f4 -d' '`
  LUN=`echo $AMCOUNTS | cut -f5 -d' '`
  rrdtool update rrd/$AM-am.rrd -t local_vlan:transit_vlan:vm:bm:lun N:$LOCAL_VLAN:$TRANSIT_VLAN:$VM:$BM:$LUN
  echo $AM $LOCAL_VLAN:$TRANSIT_VLAN:$VM:$BM:$LUN >> last_result_ams
done
