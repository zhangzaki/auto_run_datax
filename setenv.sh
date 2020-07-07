#!/bin/bash
BASED="`pwd`"
ddd="#!$BASED"
dd="$BASED/pvenv"
cd $BASED/pvenv/bin

for i in `grep pvenv/bin/python * | cut -d ":"  -f 1`
do
	sed -i "1d" $i
	sed -i "1i $ddd/pvenv/bin/python" $i
done

for n in `grep -n "#!/usr/bin/env python2.7" * |  cut -d ":" -f1`
do
	sed -i "1d" $n
	sed -i "1i $ddd/pvenv/bin/python" $n
done


for j in `grep -n pvenv * | grep -v "#" | cut -d ":" -f1`
do
	sed -i '/pvenv/ s#"[^"]*"#"'$(echo $dd)'"#' $j
done	

cd $BASED
