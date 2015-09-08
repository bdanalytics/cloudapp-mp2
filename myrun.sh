#!/bin/bash
export MYINPUT=/mp2/$2
export MYOUTPUT=/mp2/$3-output

hadoop fs -rm -r $MYOUTPUT
hadoop jar $1.jar $1 -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt $MYINPUT $MYOUTPUT
hadoop fs -cat $MYOUTPUT/part*
rm output/part*
hadoop fs -get $MYOUTPUT/part* output/

mv output/$1_output.txt output/$1_output_.txt
cat output/part-r-* > output/$1_output.txt
