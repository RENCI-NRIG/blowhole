#!/bin/sh
DATE=`date "+%Y%m%d%H%M"`
COMMIT=`git rev-parse HEAD`
SHORTCOMMIT=`git rev-parse --short=8 HEAD`

cp blowhole.spec.tmpl blowhole.spec

sed -i -e "s;@@DATE@@;${DATE};" blowhole.spec
sed -i -e "s;@@COMMIT@@;${COMMIT};" blowhole.spec
sed -i -e "s;@@SHORTCOMMIT@@;${SHORTCOMMIT};" blowhole.spec
