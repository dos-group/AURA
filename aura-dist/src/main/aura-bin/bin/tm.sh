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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/env.sh

if [ "$AURA_IDENT_STRING" = "" ]; then
    AURA_IDENT_STRING="$USER"
fi

# auxilliary function to construct a lightweight classpath for the
# Aura TaskManager
constructTaskManagerClassPath() {

    for jarfile in $AURA_LIB_DIR/*.jar ; do
        if [[ $AURA_TM_CLASSPATH = "" ]]; then
            AURA_TM_CLASSPATH=$jarfile;
        else
            AURA_TM_CLASSPATH=$AURA_TM_CLASSPATH:$jarfile
        fi
    done

    echo $AURA_TM_CLASSPATH
}

AURA_TM_CLASSPATH=`manglePathList $(constructTaskManagerClassPath)`

log=$AURA_LOG_DIR/aura-$AURA_IDENT_STRING-tm-$HOSTNAME.log
out=$AURA_LOG_DIR/aura-$AURA_IDENT_STRING-tm-$HOSTNAME.out
pid=$AURA_PID_DIR/aura-$AURA_IDENT_STRING-tm.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$AURA_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$AURA_TM_HEAP"m -Xmx"$AURA_TM_HEAP"m"

case $STARTSTOP in

    (start)
        mkdir -p "$AURA_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Task manager running as process `cat $pid` on host $HOSTNAME.  Stop it first.
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo Starting task manager on host $HOSTNAME
        $JAVA_RUN $JVM_ARGS $AURA_OPTS $log_setting -classpath $AURA_TM_CLASSPATH de.tuberlin.aura.taskmanager.TaskManager --config-dir=$AURA_CONF_DIR > "$out" 2>&1 < /dev/null &
        echo $! > $pid
    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Stopping task manager on host $HOSTNAME
                kill `cat $pid`
            else
                echo No task manager to stop on host $HOSTNAME
            fi
        else
            echo No task manager to stop on host $HOSTNAME
        fi
    ;;

    (*)
        echo Please specify start or stop
    ;;

esac
