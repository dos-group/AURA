package de.tuberlin.aura.core.iosystem;

import io.netty.channel.Channel;

import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;

public final class IOEvents {

    // Disallow instantiation.
    private IOEvents() {}

    /**
     *
     */
    public static final class IODataChannelEvent extends Event {

        public static final String IO_EVENT_INPUT_CHANNEL_CONNECTED = "IO_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String IO_EVENT_OUTPUT_CHANNEL_CONNECTED = "IO_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public IODataChannelEvent( final String type, final UUID srcTaskID, final UUID dstTaskID, final Channel channel ) {
            super( type );
            // sanity check.
            if( srcTaskID == null )
                throw new IllegalArgumentException( "srcTaskID == null" );
            if( dstTaskID == null )
                throw new IllegalArgumentException( "dstTaskID == null" );
            if( channel == null )
                throw new IllegalArgumentException( "channel == null" );

            this.srcTaskID = srcTaskID;

            this.dstTaskID = dstTaskID;

            this.channel = channel;
        }

        public final Channel channel;

        public final UUID srcTaskID;

        public final UUID dstTaskID;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append( "IOChannelEvent = {" )
                    .append( " type = " + super.type + ", " )
                    .append( " srcTaskID = " + srcTaskID.toString() + ", " )
                    .append( " dstTaskID = " + dstTaskID.toString() )
                    .append( " }" ).toString();
        }
    }

    /**
     *
     */
    public static final class IOControlChannelEvent extends Event {

        public static final String IO_EVENT_MESSAGE_CHANNEL_CONNECTED = "IO_EVENT_MESSAGE_CHANNEL_CONNECTED";

        public IOControlChannelEvent( final boolean isIncoming, final UUID srcMachineID, final UUID dstMachineID, Channel controlChannel ) {
            super( IO_EVENT_MESSAGE_CHANNEL_CONNECTED );
            // sanity check.
            if( srcMachineID == null )
                throw new IllegalArgumentException( "srcMachineID == null" );
            if( dstMachineID == null )
                throw new IllegalArgumentException( "dstMachineID == null" );
            if( controlChannel == null )
                throw new IllegalArgumentException( "controlChannel == null" );

            this.isIncoming = isIncoming;

            this.srcMachineID = srcMachineID;

            this.dstMachineID = dstMachineID;

            this.controlChannel = controlChannel;
        }

        public final boolean isIncoming;

        public final UUID srcMachineID;

        public final UUID dstMachineID;

        public final Channel controlChannel;
    }

    /**
     *
     */
    public static final class IODataEvent extends Event {

        public static final String IO_EVENT_RECEIVED_DATA = "IO_EVENT_RECEIVED_DATA";

        public IODataEvent( final DataMessage message ) {
            super( IO_EVENT_RECEIVED_DATA );
            // sanity check.
            if( message == null )
                throw new IllegalArgumentException( "message == null" );

            this.message = message;
        }

        public final DataMessage message;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append( "IOChannelEvent = {" )
                    .append( " type = " + super.type + ", " )
                    .append( " message = " + message.toString() )
                    .append( " }" ).toString();
        }
    }
}
