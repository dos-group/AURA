package de.tuberlin.aura.demo.rpc;

import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.TaskManager;

public class RPCServer {
	
	public static final Logger LOG = Logger.getRootLogger();
	
	public static final class FooImpl implements FooProtocol {

		@Override
		public Integer foo(String s, Integer q, UUID a, Integer y) {

			LOG.info( "s = " + s + ", q = " + q + ", a = " + a );
			
			return q + y + 100;
		}
	}
	
	public static void main(String[] args) {
		
		final SimpleLayout layout = new SimpleLayout();
		final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
		LOG.addAppender( consoleAppender );
				
		final TaskManager taskManager = new TaskManager( LocalDeployment.MACHINE_2_DESCRIPTOR );
		taskManager.getRPCManager().registerRPCProtocolImpl( new FooImpl(), FooProtocol.class );
		
		/*BarProtocol protocol = RPC.ProtocolCaller.getProtocolProxy( BarProtocol.class, 
				taskManager.getControlChannel( LocalDeployment.MACHINE_1_DESCRIPTOR ) );
		
		protocol.bar();*/	
	}
}
