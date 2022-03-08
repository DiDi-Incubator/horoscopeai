#!/usr/bin/env bash

BASE_DIR=$(cd `dirname $0`/..; pwd)
APP_NAME=com.didichuxing.horoscope.examples.DemoLocalService
JAR_FILE=${BASE_DIR}/horoscope-examples.jar
LOG_DIR=$BASE_DIR/logs
TMP_DIR=$BASE_DIR/tmp
CONF_DIR=$BASE_DIR/conf
MAX_MEMORY=4g
export DEFAULT_FLOW_BASE_DIR="${BASE_DIR}/flow"

function print_usage() {
  echo "$0 start|stop"
}

function start() {
    timestamp=$(date +%Y-%m-%d-%H-%M)
    [[ -d $LOG_DIR ]] || mkdir -p $LOG_DIR
    [[ -d $TMP_DIR ]] || mkdir -p $TMP_DIR
    JAVA_OPTS="-Xmx${MAX_MEMORY} -Djava.io.tmpdir=$TMP_DIR -Xloggc:${LOG_DIR}/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
    exec java $JAVA_OPTS -XX:OnOutOfMemoryError="kill -9 %p" -cp $BASE_DIR:$CONF_DIR:$JAR_FILE $APP_NAME
    echo "${APP_NAME} successfully started"
}

function stop() {
    pid_pattern="$BASE_DIR.*$APP_NAME"
    pkill -f "$pid_pattern"
    if [[ -n $(pgrep -f "$pid_pattern") ]];then
      echo "Can't stop $APP_NAME"
      exit 1
    fi
    echo "$APP_NAME already stopped"
}

function main() {
   arg=$1
   case $1 in
       start)
         start
       ;;

       stop)
         stop
       ;;

       *)
         print_usage
         exit 1
       ;;
   esac
}

main "$@"
