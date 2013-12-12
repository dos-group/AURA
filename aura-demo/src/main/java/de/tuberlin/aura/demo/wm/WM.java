package de.tuberlin.aura.demo.wm;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.workloadmanager.WorkloadManager;

public class WM {

    public static final Logger LOG = Logger.getRootLogger();

    /**
     * @param args
     */
    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        new WorkloadManager( LocalDeployment.MACHINE_5_DESCRIPTOR );
    }
}
