package de.tuberlin.aura.core.task.gates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents.IODataChannelEvent;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;

public final class OutputGate extends AbstractGate {

    // TODO: wire it together with the Andi's send mechanism!

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public OutputGate( final UUID taskID,
                       final List<TaskDescriptor> gateBinding,
                       final IEventDispatcher taskEventDispatcher ) {

        super( taskID, gateBinding );
        // sanity check.
        if( taskEventDispatcher == null )
            throw new IllegalArgumentException( "taskEventDispatcher == null" );

        // All channels are by default are closed.
        this.openChannelList = new ArrayList<Boolean>( Collections.nCopies( numChannels, false ) );

        final OutputGate self = this;

        final IEventHandler gateEventHandler = new IEventHandler() {

            @Override
            public void handleEvent( Event e ) {

                if( e instanceof IODataChannelEvent ) {
                    final IODataChannelEvent event = (IODataChannelEvent)e;

                    switch( event.type ) {

                        case IODataChannelEvent.IO_EVENT_OUTPUT_GATE_OPEN: {

                            int channelIndex = 0;
                            for( final TaskDescriptor td :  self.gateBinding ) {
                                if( event.srcTaskID.equals( td.uid ) ) {
                                    self.openChannelList.set( channelIndex, true );
                                    break;
                                }
                                ++channelIndex;
                            }

                            // check postcondition.
                            if( channelIndex == self.numChannels - 1 )
                                throw new IllegalStateException( "could not find channel to be opened" );

                        } break;

                        case IODataChannelEvent.IO_EVENT_OUTPUT_GATE_CLOSE: {

                            int channelIndex = 0;
                            for( final TaskDescriptor td :  self.gateBinding ) {
                                if( event.srcTaskID.equals( td.uid ) ) {
                                    self.openChannelList.set( channelIndex, false );
                                    break;
                                }
                                ++channelIndex;
                            }

                            // check postcondition.
                            if( channelIndex == self.numChannels - 1 )
                                throw new IllegalStateException( "could not find channel to be closed" );

                        } break;

                        default: {
                            throw new IllegalStateException( "not allowed to handle: " + e.type  );
                        }
                    }
                }
            }
        };

        taskEventDispatcher.addEventListener( IODataChannelEvent.IO_EVENT_OUTPUT_GATE_OPEN, gateEventHandler );
        taskEventDispatcher.addEventListener( IODataChannelEvent.IO_EVENT_OUTPUT_GATE_CLOSE, gateEventHandler );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( OutputGate.class );

    private final List<Boolean> openChannelList;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    public void writeDataToChannel( int channelIndex, DataMessage dataMessage ) {
        // sanity check.
        if( dataMessage == null )
            throw new IllegalArgumentException( "message == null" );

        if( isGateOpen( channelIndex ) ) {
            LOG.error( "channel is closed" );
        }

        // TODO: change insert in output queue!
        getChannel( channelIndex ).writeAndFlush( dataMessage );
    }

    public boolean isGateOpen( int channelIndex ) {
        return openChannelList.get( channelIndex );
    }
}
