#!/usr/bin/env bash

BASE_DIR=$(cd `dirname $0`/..; pwd)
COOKIE=$2
KEY=$3
SECRET=$4
function print_usage() {
  echo "$0 start -cookie xxx -key xxx -secret xxx"
}

function start() {
    python $BASE_DIR/demo/weibo.py  \
    --cookie "${COOKIE}"\
    && curl --location --request POST 'http://localhost:8062/api/schedule/info/_/now/demo/pipeline' \
    --header 'Content-Type: application/json' \
    --data-raw "{ 'is_comments':true,  'stock_id' : 'sz000001',
                  'scale' : '60',   'count' : '20',
                  'key' : "${KEY}",
                  'secret' : "${SECRET}" }"
}


function main() {
   case $1 in
       start)
         start
       ;;

       *)
         print_usage
         exit 1
       ;;
   esac
}

main "$@"