# OVERVIEW

Blowhole subscribes to a number of slice lists on an indicated
XMPP server (using provided credentials) and listens for the events on the slice manifests
and slice lists. When slice state is updated (its manifest is republished) a generated event
tells the software to invoke one or more of the included plugin 'workers' to process the manifest. 

The workers are: 

1. GMOCWorker (obsoleted now)

With an http url, the worker converts manifest to RSpec and does HTTP PUT. With file URL it writes 
the manifest to a file in a file indicated by the URL (except the file name has '-' and slice urn appended to it).

2. XODbWorker

Saves slice information into the central ExoGENI database

3. GENIWorker

Parses the manifest (as RSpec and raw NDL) and populates the monitoring database

## BUILDING

The easy way to build (needs maven):

$ mvn clean package

produces an app that can be installed. The files are under target/appassembler/[bin/:repo/] - 
can be tar'ed/zipped etc. Creates both windows and unix-friendly start up shell scripts.

To run the app:

$ sh target/appassembler/bin/blowhole

When you run the first time it will complain that it can't find the config file and print out 
a sample file. The file goes either into /etc/blowhole/xmpp.properties or $HOME/.xmpp.properties.

Other ways to build/run:

$ mvn clean assembly:assembly

produces jar with dependencies. Then to run

java -jar ./target/pubsub-daemon-0.1-SNAPSHOT-jar-with-dependencies.jar

It can be run alternatively: 

java -cp ./target/pubsub-daemon-0.1-SNAPSHOT-jar-with-dependencies.jar org.renci.pubsub_daemon.ManifestSubscriber

To stop use Ctrl-C (note that it may take a few seconds to cancel active subscriptions).

## TOOLS

---- Listing nodes in pubsub space 

To list all nodes, if you built it using 'mvn clean package', then simply

$ sh target/appassembler/bin/listNodes

---- Deleting nodes in pubsub space 

To delete all pubsub nodes (must be run on behalf of the publisher, not the subscribers), if you built it
using 'mvn clean package appassembler:assemble' above, then simply

$ sh target/appassembler/bin/deleteNodes

---- Database 

Blowhole can save all slices into a MySQL database. The schema is xoschema.sql. You need to provide three properties
DB.url, DB.user and DB.password for blowhole to start saving slices into the db.

## FOR PACKAGERS

$ mvn clean package

will create a deployable JSW-based daemon under

./target/generated-resources/appassembler/jsw/blowholed

It should be suitable for packaging in RPM. Right now only linux (32- and 64-bit linux distros).

You can also package the individual applications produced with 'mvn clean package appassembler:assemble'

## Building RPM
To build an RPM
```
mkdir ~/blowhole
cd ~/blowhole
git clone https://github.com/RENCI-NRIG/blowhole.git
export JAVA_HOME=/usr/java/latest
export PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$LOCAL_DEV/ant/bin:$LOCAL_DEV/maven/bin:$PATH
export ANT_OPTS="-Xms40m -Xmx1024m"
export MAVEN_OPTS="-Xms40m -Xmx1024m"
export SHORTCOMMIT=`git rev-parse --short=8 HEAD`
cd blowhole # You’re now entering the source you just checked out
./create_spec_from_tmpl.sh
cd ../ # This will take you back to the top level blowhole directory
mv blowhole blowhole-0.2-${SHORTCOMMIT}
tar -cvzf blowhole-0.2-${SHORTCOMMIT}.tar.gz blowhole-0.2-${SHORTCOMMIT}
rpmbuild -ta blowhole-0.2-${SHORTCOMMIT}.tar.gz
```
