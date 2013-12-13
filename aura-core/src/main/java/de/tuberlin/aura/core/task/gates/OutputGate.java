package de.tuberlin.aura.core.task.gates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents.IODataChannelEvent;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.core.task.common.TaskContext;

public final class OutputGate extends AbstractGate {

    // TODO: wire it together with the Andi's send mechanism!

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public OutputGate( final TaskContext context, int gateIndex ) {
        super( context, gateIndex, context.taskBinding.outputGateBindings.get( gateIndex ).size() );

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
                            self.openChannelList.set( context.getInputChannelIndexFromTaskID( event.srcTaskID ), true );
                        } break;

                        case IODataChannelEvent.IO_EVENT_OUTPUT_GATE_CLOSE: {
                            self.openChannelList.set( context.getInputChannelIndexFromTaskID( event.srcTaskID ), false );
                        } break;

                        default: {
                            throw new IllegalStateException( "not allowed to handle: " + e.type  );
                        }
                    }
                }
            }
        };

        context.dispatcher.addEventListener( IODataChannelEvent.IO_EVENT_OUTPUT_GATE_OPEN, gateEventHandler );
        context.dispatcher.addEventListener( IODataChannelEvent.IO_EVENT_OUTPUT_GATE_CLOSE, gateEventHandler );
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
