package de.tuberlin.aura.core.task.gates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
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

                if( e instanceof DataIOEvent ) {
                    final DataIOEvent event = (DataIOEvent)e;

                    switch( event.type ) {

                        case DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN: {
                            self.openChannelList.set( context.getInputChannelIndexFromTaskID( event.srcTaskID ), true );
                        } break;

                        case DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE: {
                            self.openChannelList.set( context.getInputChannelIndexFromTaskID( event.srcTaskID ), false );
                        } break;

                        default: {
                            throw new IllegalStateException( "not allowed to handle: " + e.type  );
                        }
                    }
                }
            }
        };

        context.dispatcher.addEventListener( DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, gateEventHandler );
        context.dispatcher.addEventListener( DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, gateEventHandler );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( OutputGate.class );

    private final List<Boolean> openChannelList;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    public void writeDataToChannel( final int channelIndex, final DataBufferEvent data ) {
        // sanity check.
        if( data == null )
            throw new IllegalArgumentException( "data == null" );

        if( isGateOpen( channelIndex ) ) {
            LOG.error( "channel is closed" );
        }

        // TODO: change insert in output queue!
        getChannel( channelIndex ).writeAndFlush( data );
    }

    public boolean isGateOpen( final int channelIndex ) {
        return openChannelList.get( channelIndex );
    }
}
