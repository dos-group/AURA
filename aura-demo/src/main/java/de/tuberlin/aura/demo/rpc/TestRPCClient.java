package de.tuberlin.aura.demo.rpc;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.TaskManager;

public class TestRPCClient {

    public static final Logger LOG = Logger.getRootLogger();

    public static final class BarImpl implements BarProtocol {

        @Override
        public void bar() {
            LOG.info( "bar on client" );
        }
    }

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        final TaskManager taskManager = new TaskManager( LocalDeployment.MACHINE_1_DESCRIPTOR );
        taskManager.getRPCManager().registerRPCProtocolImpl( new BarImpl(), BarProtocol.class );
        taskManager.getRPCManager().connectToMessageServer( LocalDeployment.MACHINE_2_DESCRIPTOR );

        FooProtocol protocol = taskManager.getRPCManager()
                .getRPCProtocolProxy( FooProtocol.class, LocalDeployment.MACHINE_2_DESCRIPTOR );

        //int i = protocol.foo( "foo on server", 10, UUID.randomUUID(), 5 );
        //LOG.info( "i = " + i );

        protocol.foo1( null );
    }
}
