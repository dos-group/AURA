package de.tuberlin.aura.demo.tm;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.TaskManager;

public class TM3 {

    public static final Logger LOG = Logger.getRootLogger();

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        final RPCManager rpcManager = new TaskManager("localhost:2181",  LocalDeployment.MACHINE_3_DESCRIPTOR ).getRPCManager();
        rpcManager.connectToMessageServer( LocalDeployment.MACHINE_5_DESCRIPTOR );

    }
}
