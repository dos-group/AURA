package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractGate {

	public AbstractGate( final int numChannels ) {
		// sanity check.
		if( numChannels < 0 )
			throw new IllegalArgumentException( "numChannels < 0" );
	
		this.numChannels = numChannels;
		
		if( numChannels > 0 ) {
			
			channels = new ArrayList<Channel>( Collections.nCopies( numChannels, (Channel)null ) );
			
		} else { // numChannels == 0
			
			channels = null;
		}
	}
	
	protected final int numChannels;
	
	protected final List<Channel> channels;
	
	public void setChannel( int channelIndex, final Channel channel ) {
		// sanity check.
		if( channelIndex < 0 )
			throw new IllegalArgumentException( "channelIndex < 0" );
		if( channelIndex >= numChannels )
			throw new IllegalArgumentException( "channelIndex >= numChannels" );
		if( channels == null )
			throw new IllegalStateException( "channels == null" );
		
		channels.set( channelIndex, channel ); 
	}
	
	public Channel getChannel( int channelIndex ) {
		// sanity check.
		if( channelIndex < 0 )
			throw new IllegalArgumentException( "channelIndex < 0" );
		if( channelIndex >= numChannels )
			throw new IllegalArgumentException( "channelIndex >= numChannels" );
		if( channels == null )
			throw new IllegalStateException( "channels == null" );
		
		return channels.get( channelIndex );
	}
}
