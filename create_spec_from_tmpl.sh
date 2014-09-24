#!/bin/sh
DATE=`date "+%Y%m%d"`
GLOBALREV=`svnversion`

cp blowhole.spec.tmpl blowhole.spec

sed -i -e "s;@@DATE@@;${DATE};" blowhole.spec
sed -i -e "s;@@GLOBALREV@@;${GLOBALREV};" blowhole.spec
