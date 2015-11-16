#! /bin/bash
#######################################################################
# Merges the nominal "service" attribute in the header of ARFF files, #
# to make them mutually compatible for Weka training and testing.     #
#######################################################################

arffs=$@
sumserv=`mktemp`
for ar in $arffs; do 
  grep service $ar | grep -o "\{.*\}" | tr -d \{\} | tr "," "\n" >> $sumserv && echo "reading services from $ar..."
done

merged=`sort $sumserv | uniq | grep -v ^$`

marray="@ATTRIBUTE service\t{"
first=1
for s in $merged; do
  (( $first == 1 )) || marray="${marray}," 
  first=0
  marray=${marray}${s}
done
marray=${marray}"}"


for arff in $arffs; do
    newname=`echo $arff | sed s:tidy:mrg:`
    sed   s:'^\@ATTRIBUTE\ service.*$':"$marray": $arff > $newname
    echo "Saving $newname..."
done

echo
echo "Merged service attributes for $arffs"
