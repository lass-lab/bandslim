#!/bin/bash

TEMP1=`sudo fdisk -l | grep -n Cosmos+`
TEMP2=`echo $TEMP1 | cut -d ':' -f1`
TEMP3=`sudo fdisk -l | head -$(expr $TEMP2 - 1) | tail -1`
TEMP4=`echo $TEMP3 | cut -d ':' -f1`
TEMP5=`echo $TEMP4 | cut -d ' ' -f2`

echo "Run DB_BENCH (iLSM)"

if [ $# -ne 3 ]
then
    echo -e "Usage:\n sudo ./test.sh <fillseq/fillrandom/readseq/readrandom> <value_size> <#_of_pairs> [optional] \n"
    exit 0
fi

sudo ./db_bench --benchmarks="$1" -use_direct_io_for_flush_and_compaction=true --use_direct_reads=true --num=$3 --key_size=4 --value_size=$2 --db="/mnt/DB" --compression_ratio=1 --level0_file_num_compaction_trigger=2 --ilsm_device_path="$TEMP5" #--stats_interval=1

