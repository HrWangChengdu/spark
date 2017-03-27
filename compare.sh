#!/bin/bash

src0=/users/haoranw2/Tools/spark-latest/
src1=/users/haoranw2/Tools/spark-my-fork/

dir=core/src/main/scala/org/apache/spark/

file0=rdd/CoGroupedRDD.scala
file1=util/collection/ExternalAppendOnlyMap.scala
file2=util/collection/SizeTrackingAppendOnlyMap.scala
file3=util/collection/AppendOnlyMap.scala
file4=util/collection/CompactBuffer.scala
file5=util/collection/Spillable.scala

rm diff.out

#for file in $file0 $file1 $file2 $file3 $file4; do
for file in $file2 $file3; do
    echo $file >> diff.out
    path0=$src0$dir$file
    path1=$src1$dir$file
    diff $path0 $path1 >> diff.out
done
