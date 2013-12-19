package de.tuberlin.aura.demo.tm;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.TaskManager;

public class TM4 {

    public static final Logger LOG = Logger.getRootLogger();

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        final IOManager ioManager = new TaskManager( LocalDeployment.MACHINE_4_DESCRIPTOR ).getIOManager();
        ioManager.connectMessageChannelBlocking( LocalDeployment.MACHINE_5_DESCRIPTOR );
    }
}
