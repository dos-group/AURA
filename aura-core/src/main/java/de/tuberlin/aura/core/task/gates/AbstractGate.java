package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;

public abstract class AbstractGate {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public AbstractGate( final UUID taskID, final List<TaskDescriptor> gateBinding ) {
        // sanity check.
        if( taskID == null )
            throw new IllegalArgumentException( "taskID == null" );
        if( gateBinding == null )
            throw new IllegalArgumentException( "gateBinding == null" );

        this.taskID= taskID;

        this.numChannels = gateBinding.size();

        if( numChannels > 0 ) {
            channels = new ArrayList<Channel>( Collections.nCopies( numChannels, (Channel)null ) );
        } else { // numChannels == 0
            channels = null;
        }

        this.gateBinding = gateBinding;
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    protected final UUID taskID;

    protected final int numChannels;

    protected final List<Channel> channels;

    protected final List<TaskDescriptor> gateBinding;

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
