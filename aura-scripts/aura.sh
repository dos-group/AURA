#!/bin/sh
USER="andreas.kunft"
URL="cit.tu-berlin.de"

LOCAL_AURA_PATH="/home/akunft/dev/AURA"
LOCAL_AURA_LOGS_DESTINATION="/home/akunft/aura_measurements"
HOME_PATH="/home/$USER"
AURA_PATH="$HOME_PATH/aura"
DATA_PATH="/data/$USER"
AURA_DATA_PATH="$DATA_PATH/aura"

LOCAL_AURA_INPUT="/home/teots/Desktop/text"
AURA_INPUT_PATH="$AURA_DATA_PATH/input"

BENCHMARK_PATH="$DATA_PATH/benchmarks"
ZOOKEEPER_PATH="$HOME_PATH/zookeeper/zookeeper-3.4.5"

ZOOKEEPERS=1

# Check whether the given ranges are numbers.
if ! echo $2 | egrep -q '^[0-9]+$'; then
	echo "\${FROM} isn't a number."
	exit 1
elif [ $2 -lt 1 ] || [ $2 -gt 200 ]; then
	echo "\${FROM} isn't in the range [1, 200]."
	exit 1
fi

# Check whether the given ranges are numbers.
if ! echo $3 | egrep -q '^[0-9]+$'; then
	echo "\${TO} isn't a number."
	exit 1
elif [ $3 -lt 1 ] || [ $3 -gt 200 ]; then
	echo "\${TO} isn't in the range [1, 200]."
	exit 1
elif [ $2 -gt $3 ]; then
	echo "\${FROM} is bigger than \${TO}."
	exit 1
elif [ $(($3- ($2 - 1))) -lt $ZOOKEEPERS ]; then
	echo "Not enough servers."
	exit 1
fi
		
# First server will run nimbus
WORKLOADMANAGER="wally`printf "%03d" $2`.$URL"
WALLYMASTER="wally-master.cit.tu-berlin.de"

# First servers will run zookeeper
ZOOKEEPER_SERVERS=""
for i in $(seq $2 $(($2 + $ZOOKEEPERS - 1))); do
	ZOOKEEPER_SERVERS="$ZOOKEEPER_SERVERS      - \"wally`printf "%03d" $i`.$URL\"
" # the newline must be added this way, because "ed" doesn't interpret \n
done

ZOOKEEPER_SERVERS2=""
for i in $(seq $2 $(($2 + $ZOOKEEPERS - 1))); do
	NEW="server.$(($i - ($2 - 1)))=wally`printf "%03d" $i`.$URL:2888:3888"
	ZOOKEEPER_SERVERS2="$ZOOKEEPER_SERVERS2$NEW 
" # the newline must be added this way, because "ed" doesn't interpret \n
done

ZOOKEEPER_SERVERS3=""
for i in $(seq $2 $(($2 + $ZOOKEEPERS - 1))); do
	NEW="wally`printf "%03d" $i`.$URL:2181"
	if [ $i -eq $2 ]; then
		ZOOKEEPER_SERVERS3="$ZOOKEEPER_SERVERS3$NEW"
	else
		ZOOKEEPER_SERVERS3="$ZOOKEEPER_SERVERS3,$NEW"
	fi
done

ZOOKEEPER_SETUP_COMMAND="ed $ZOOKEEPER_PATH/conf/zoo.cfg << EDEND
\$
7,.c
$ZOOKEEPER_SERVERS2
.
w
q
EDEND
"

case $1 in
	# Install
	install)
		echo "Install Aura..."

		ssh -t -t "$USER@$WALLYMASTER" << SSHEND
	rm -rf $AURA_PATH/
	mkdir -p $AURA_PATH/
	exit
SSHEND

		scp -r $LOCAL_AURA_PATH/* $USER@$WALLYMASTER:$AURA_PATH/ > /dev/null

		ssh -t -t "$USER@$WALLYMASTER" << SSHEND
cd $AURA_PATH/
mvn clean install
exit
SSHEND

		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"

			ssh -t -t "$USER@$ADDRESS" << SSHEND
mkdir -p $AURA_DATA_PATH/
mkdir -p $AURA_DATA_PATH/logs
mkdir -p $AURA_DATA_PATH/data
rm -rf $AURA_INPUT_PATH/
mkdir -p $AURA_INPUT_PATH
mkdir -p $DATA_PATH/zookeeper/data
mkdir -p $BENCHMARK_PATH
exit
SSHEND
		
			#scp -r $LOCAL_AURA_INPUT/* $USER@$ADDRESS:$AURA_INPUT_PATH/ > /dev/null

		done		

	;;
	# Setup
	setup)
		echo "Setting up Strom..."
		
		# Setup zookeeper on all machines
		ssh -t -t "$USER@$WORKLOADMANAGER" << SSHEND
$ZOOKEEPER_SETUP_COMMAND
exit
SSHEND

		for i in $(seq $2 $(($2 + $ZOOKEEPERS))); do
			ADDRESS="wally`printf "%03d" $i`.$URL"		
			
			ssh -t -t "$USER@$ADDRESS" << SSHEND
echo $(($i - ($2 - 1))) > $DATA_PATH/zookeeper/data/myid 
exit
SSHEND
		done
	;;
	# Start
	start)
		echo "Start TaskManagers..."
		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"
			printf "Start TaskManager on $ADDRESS ... "

			# Start Zookeeper	
			if [ $i -lt $(($2 + $ZOOKEEPERS)) ]; then
				echo "START ZOOKEEPER"
				ssh -t -t "$USER@$ADDRESS" << SSHEND
sh $ZOOKEEPER_PATH/bin/zkServer.sh start
exit
SSHEND
			fi	

			# Start WorkloadManger
			if [ $i -eq $2 ]; then
				ssh -t -t "$USER@$ADDRESS" << SSHEND
cd aura/aura-wm/
export MAVEN_OPTS="-Xms4G -Xmx8G -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9011"
nohup mvn exec:java -Dexec.mainClass="de.tuberlin.aura.workloadmanager.WorkloadManager" -Dexec.args="$ZOOKEEPER_SERVERS3 10000 11111 $AURA_DATA_PATH/logs" > $AURA_DATA_PATH/logs/log 2>&1 &
echo \$! > $AURA_DATA_PATH/data/wm_pid
exit
SSHEND
				sleep 5
			# Start TaskManager 
			else
				ssh -t -t "$USER@$ADDRESS" << SSHEND
cd aura/aura-tm/
export MAVEN_OPTS="-Xms4G -Xmx8G -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9011"
nohup mvn exec:java -Dexec.mainClass="de.tuberlin.aura.taskmanager.TaskManager" -Dexec.args="$ZOOKEEPER_SERVERS3 10000 11111 $AURA_DATA_PATH/logs" > $AURA_DATA_PATH/logs/log 2>&1 &
echo \$! > $AURA_DATA_PATH/data/tm_pid
exit
SSHEND
			fi
			printf "done\n"
		done
	;;
	# Get the benchmarks
	get_benchmarks)
		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"
			printf "Get benchmarks from $ADDRESS ... "
			
			mkdir -p $LOCAL_AURA_LOGS_DESTINATION/$ADDRESS
			scp -r $USER@$ADDRESS:$AURA_DATA_PATH/logs/* $LOCAL_AURA_LOGS_DESTINATION/$ADDRESS > /dev/null
			
			printf "done\n"
		done
	;;	
	# Cleanup benchmarks
	cleanup_benchmarks)
		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"
			printf "Cleanup benchmarks on $ADDRESS ... "
			
			ssh -t -t "$USER@$ADDRESS" << SSHEND
cat /dev/null > $AURA_DATA_PATH/logs/log
exit
SSHEND
		done
	;;
	# Stop
	stop)
		echo "Stop Aura..."
		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"
			
			if [ $i -eq $2 ]; then
				ssh -t -t "$USER@$ADDRESS" << SSHEND
kill \`cat $AURA_DATA_PATH/data/wm_pid\`
exit
SSHEND
			else
				ssh -t -t "$USER@$ADDRESS" << SSHEND
kill \`cat $AURA_DATA_PATH/data/tm_pid\`
exit
SSHEND
			fi
		
			if [ $i -lt $(($2 + $ZOOKEEPERS)) ]; then
				ssh -t -t "$USER@$ADDRESS" << SSHEND
sh $ZOOKEEPER_PATH/bin/zkServer.sh stop
exit
SSHEND
			fi
		done
	;;
	cleanup)
	echo "Cleanup Aura workers..."
		for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"
			printf "Cleanup Aura on $ADDRESS ... "
			
			if [ $i -lt $(($2 + $ZOOKEEPERS)) ]; then
			ssh -t -t "$USER@$ADDRESS" << SSHEND
rm -rf $DATA_PATH/zookeeper/data/*
exit
SSHEND
			fi
			
			ssh -t -t "$USER@$ADDRESS" << SSHEND
rm -rf $BENCHMARK_PATH/*
rm -rf $AURA_DATA_PATH/data/*
rm -rf $AURA_DATA_PATH/logs/*
exit
SSHEND
		printf "done\n"
		done
	;;
	kill)
	for i in $(seq $2 $3); do
			ADDRESS="wally`printf "%03d" $i`.$URL"

			ssh -t -t "$USER@$ADDRESS" << SSHEND
pkill -u $USER
exit
SSHEND
			done
	;;
	*)
		echo "Usage: sh aura.sh \${OPERATION} (\${FROM}) (\${TO})"
		echo "Operations:"
		echo "install\t\tInstall Storm on all wally servers in the given range."
		echo "setup\t\tConfigures Storm on all wally servers in the given range."
		echo "start\t\tStarts the Zookeeper cluster, Nimbus, and the workers on all wally servers in the given range."
		echo "stop\t\tStops the Zookeeper cluster, Nimbus, and the workers on all wally servers in the given range."
		echo "cleanup\t\tDeletes all log files on all wally servers in the given range."
	;;
esac
