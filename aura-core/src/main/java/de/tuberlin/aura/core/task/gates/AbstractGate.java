package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.tuberlin.aura.core.task.common.TaskContext;

public abstract class AbstractGate {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public AbstractGate( final TaskContext context, int gateIndex, int numChannels ) {
        // sanity check.
        if( context == null )
            throw new IllegalArgumentException( "context == null" );

        this.context= context;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;

        if( numChannels > 0 ) {
            channels = new ArrayList<Channel>( Collections.nCopies( numChannels, (Channel)null ) );
        } else { // numChannels == 0
            channels = null;
        }
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    protected TaskContext context;

    protected final int numChannels;

    protected final int gateIndex;

    protected final List<Channel> channels;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

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
