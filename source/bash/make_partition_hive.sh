#!/bin/bash

function init_env(){
    HIVE="/app/hive/bin/hive"
    PROGRAM_NAME=`/bin/basename $0`
    CURRENT_DIR=`/bin/pwd`
}

function check_parameter(){
    FROM_DATE=""
    TO_DATE=""
    DB=""
    TABLE=""
    HOURLY=true
    HDFS_PATH=""

    local options
    options=`/usr/bin/getopt -o f:t:d:hlp: -l help,from:,to:,table:,hourly,daily,path: -n "$PROGRAM_NAME" -- $*`
    if [ $? -ne 0 ]; then
        return 1
    fi

    eval set -- "$options"
    while true; do
        case "$1" in
        --help)
        print_usage
        exit 0
        ;;
            -f|--from)
                FROM_DATE=$2
                shift 2
                ;;
            -t|--to)
                TO_DATE=$2
                shift 2
                ;;
            -d|--table)
                DB=`echo $2 | cut -d . -f 1`
                TABLE=`echo $2 | cut -d . -f 2`
                shift 2
                ;;
            -h|--hourly)
                HOURLY=true
                shift
                ;;
            -l|--daily)
        HOURLY=false
        shift
        ;;
        -p|--path)
        HDFS_PATH=$2
        shift 2
        ;;
        --)
                shift
                break
                ;;
        esac
    done
    return 0
}
function print_usage(){
    /bin/cat << EOF
Usage: $PROGRAM_NAME [OPTION]...
run make partition
    -f, --from=FROM_DATE yyyyMMddHH
    -t, --to=TO_DATE yyyyMMddHH
    -d, --table=DB.TABLE
    -h, --hourly partition
    -d, --daily partition
    -p, --path HDFS path
    --help
EOF
}
function make_partition(){
    local move=1
    local format="%Y%m%d%H"
    local output_format="%Y/%m/%d/%H"
    local formatString="${FROM_DATE:0:8} ${FROM_DATE:8:2}"
    local unit="hour"
    local to_date=$TO_DATE
    local partition_unit="part_hour"
    local query_file=`/bin/pwd`"/$DB-$TABLE.hql"
    echo "make partition"
    if [ $HOURLY = false ]; then
        format="%Y%m%d"
        output_format="%Y/%m/%d"
        formatString="${FROM_DATE:0:8}"
        unit="day"
        to_date="${TO_DATE:0:8}"
    partition_unit="part_date"
    fi

    local time_from=`/bin/date -d "${formatString}" +"${format}"`
    if [ -f $query_file ]; then
    rm -rf $query_file
    fi
    echo "make partition sql file: $query_file"
    echo "use $DB;" >> $query_file
    while [[ $time_from -lt $to_date ]]; do
        local time_to_format="${time_from:0:8}"
        if [ $HOURLY = true ]; then
            time_to_format="${time_from:0:8} ${time_from:8:2}"
        fi
        time_to_output_st1=`/bin/date -d "${time_to_format}" +"${format}"`
        time_to_output_st2=`/bin/date -d "${time_to_format}" +"${output_format}"`
    echo "alter table $TABLE add if not exists partition (${partition_unit}='${time_to_output_st1}') location '${HDFS_PATH}/${time_to_output_st2}';" >> $query_file
        time_to=`/bin/date -d "${time_to_format} +$move $unit" +"${format}"`
        time_from=$time_to
    done
    /usr/bin/head $query_file
    echo -e ".\n.\n.\n"
    echo "Make Partition Continue.. (y/n):"
    read -r go
    if [ $go == "y" ] || [ $go == "Y" ]; then
    $HIVE -f $query_file
    else
    echo "Bye Bye!!"
    exit 1
    fi
}

init_env
check_parameter $*
make_partition