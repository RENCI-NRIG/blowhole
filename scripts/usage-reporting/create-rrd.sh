# 10 years, every 3 hours, maximums for ten years, every period, daily and monthly
#ExoSM

. ./environment.sh
echo -n "Creating ExoSM"
if [ ! -e rrd/exo-sm.rrd ]; then
	rrdtool create rrd/exo-sm.rrd --start N --step 10800 DS:local_vlan:GAUGE:21600:0:5000 DS:transit_vlan:GAUGE:21600:0:5000 DS:global_vlan:GAUGE:21600:0:5000 DS:mp_vlan:GAUGE:21600:0:5000 DS:vm:GAUGE:21600:0:5000 DS:bm:GAUGE:21600:0:5000 DS:lun:GAUGE:21600:0:5000 RRA:MAX:0.5:1:29200 RRA:MAX:0.5:8:3650 RRA:MAX:0.5:240:120
	echo done
else
	echo already exists
fi

# others
for SM in $SMLIST; do
	echo -n Creating $SM RRD
	if [ ! -e rrd/$SM-sm.rrd ]; then 
		rrdtool create rrd/$SM-sm.rrd --start N  --step 10800 DS:local_vlan:GAUGE:21600:0:5000 DS:transit_vlan:GAUGE:21600:0:5000 DS:vm:GAUGE:21600:0:5000 DS:bm:GAUGE:21600:0:5000 DS:lun:GAUGE:21600:0:5000 RRA:MAX:0.5:1:29200 RRA:MAX:0.5:8:3650 RRA:MAX:0.5:240:120
		echo done
	else	
		echo already exists
	fi
done

# test rrd
#rrdtool create rrd/test-sm.rrd --start N --step 10 DS:local_vlan:GAUGE:21600:0:5000 DS:transit_vlan:GAUGE:21600:0:5000 DS:global_vlan:GAUGE:21600:0:5000 DS:mp_vlan:GAUGE:21600:0:5000 DS:vm:GAUGE:21600:0:5000 DS:bm:GAUGE:21600:0:5000 DS:lun:GAUGE:21600:0:5000 RRA:MAX:0.5:1:29200 RRA:MAX:0.5:8:3650 RRA:MAX:0.5:240:120

# AMs
for AM in $AMLIST; do
	echo -n Creating $AM RRD
	if [ ! -e rrd/$AM-am.rrd ]; then
		rrdtool create rrd/$AM-am.rrd --start N  --step 10800 DS:local_vlan:GAUGE:21600:0:5000 DS:transit_vlan:GAUGE:21600:0:5000 DS:vm:GAUGE:21600:0:5000 DS:bm:GAUGE:21600:0:5000 DS:lun:GAUGE:21600:0:5000 RRA:MAX:0.5:1:29200 RRA:MAX:0.5:8:3650 RRA:MAX:0.5:240:120
		echo done
	else
		echo already exists
	fi
done
