#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Aura project (http://aura.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

STARTSTOP=$1
EXECUTIONMODE=$2

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/env.sh

if [ "$EXECUTIONMODE" = "local" ]; then
    AURA_WM_HEAP=`expr $AURA_WM_HEAP + $AURA_TM_HEAP`
    AURA_WM_MODE="LOCAL"
else
    AURA_WM_MODE="DISTRIBUTED"
fi

JVM_ARGS="$JVM_ARGS -Xms"$AURA_WM_HEAP"m -Xmx"$AURA_WM_HEAP"m"

if [ "$AURA_IDENT_STRING" = "" ]; then
    AURA_IDENT_STRING="$USER"
fi

# auxilliary functionTypeName to construct a the classpath for the wm
constructWorkManagerClassPath() {
    for jarfile in $AURA_LIB_DIR/*.jar ; do
        if [[ $AURA_WM_CLASSPATH = "" ]]; then
            AURA_WM_CLASSPATH=$jarfile;
        else
            AURA_WM_CLASSPATH=$AURA_WM_CLASSPATH:$jarfile
        fi
    done

    echo $AURA_WM_CLASSPATH
}

AURA_WM_CLASSPATH=`manglePathList $(constructWorkManagerClassPath)`

log=$AURA_LOG_DIR/aura-$AURA_IDENT_STRING-wm-$HOSTNAME.log
out=$AURA_LOG_DIR/aura-$AURA_IDENT_STRING-wm-$HOSTNAME.out
pid=$AURA_PID_DIR/aura-$AURA_IDENT_STRING-wm.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$AURA_CONF_DIR"/log4j.properties"

case $STARTSTOP in

    (start)
        mkdir -p "$AURA_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Workload manager running as process `cat $pid`.  Stop it first.
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo Starting workload manager
        $JAVA_RUN $JVM_ARGS $AURA_OPTS $log_setting -classpath $AURA_WM_CLASSPATH de.tuberlin.aura.workloadmanager.WorkloadManager --config-dir=$AURA_CONF_DIR --mode=$AURA_WM_MODE  > "$out" 2>&1 < /dev/null &
        echo $! > $pid
    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Stopping workload manager
                kill `cat $pid`
            else
                echo No workload manager to stop
            fi
        else
            echo No workload manager to stop
        fi
    ;;

    (*)
        echo Please specify start or stop
    ;;

esac
