package de.tuberlin.aura.core.task.gates;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;

public final class InputGate extends AbstractGate {
	
	public InputGate( final int numInputChannels ) {
		super( numInputChannels );
		
		if( numChannels > 0 ) {
			
			inputQueue = new LinkedBlockingQueue<DataMessage>();
			
		} else { // numChannels == 0
			
			inputQueue = null;
		}
	}
	
	private final BlockingQueue<DataMessage> inputQueue;
	
	public BlockingQueue<DataMessage> getInputQueue() {
		return inputQueue;
	}
	
	public void addToInputQueue( final DataMessage message ) {
		// sanity check.
		if( message == null )
			throw new IllegalArgumentException( "message == null" );
		
		inputQueue.add( message );
	}
}
