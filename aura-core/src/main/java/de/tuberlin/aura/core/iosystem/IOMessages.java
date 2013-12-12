package de.tuberlin.aura.core.iosystem;

import java.io.Serializable;
import java.util.UUID;

public final class IOMessages {

    // Disallow instantiation.
    private IOMessages() {}

    /**
     *
     */
    public static final class ChannelHandshakeMessage implements Serializable {

        private static final long serialVersionUID = 4015271410077813454L;

        public ChannelHandshakeMessage( final UUID srcTaskID, final UUID dstTaskID ) {
            // sanity check.
            if( srcTaskID == null )
                throw new IllegalArgumentException( "srcTaskID == null" );
            if( dstTaskID == null )
                throw new IllegalArgumentException( "dstTaskID == null" );

            this.srcTaskID = srcTaskID;

            this.dstTaskID = dstTaskID;
        }

        public UUID srcTaskID;

        public UUID dstTaskID;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append( "ChannelHandshakeMessage = {" )
                    .append( " srcTaskID = " + srcTaskID.toString() + ", " )
                    .append( " dstTaskID = " + dstTaskID.toString() )
                    .append( " }" ).toString();
        }
    }

    /**
     *
     */
    public static final class DataMessage implements Serializable {

        private static final long serialVersionUID = 1969518766756038834L;

        public DataMessage( final UUID messageID, final UUID srcTaskID, final UUID dstTaskID, final byte[] data ) {
            // sanity check.
            if( messageID == null )
                throw new IllegalArgumentException( "messageID == null" );
            if( srcTaskID == null )
                throw new IllegalArgumentException( "srcTaskID == null" );
            if( dstTaskID == null )
                throw new IllegalArgumentException( "dstTaskID == null" );
            if( data == null )
                throw new IllegalArgumentException( "data == null" );

            this.messageID = messageID;

            this.srcTaskID = srcTaskID;

            this.dstTaskID = dstTaskID;

            this.data = data;
        }

        public UUID messageID;

        public UUID srcTaskID;

        public UUID dstTaskID;

        public byte[] data;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append( "ChannelHandshakeMessage = {" )
                    .append( " messageID = " + srcTaskID.toString() + ", " )
                    .append( " srcTaskID = " + srcTaskID.toString() + ", " )
                    .append( " dstTaskID = " + dstTaskID.toString() + ", " )
                    .append( " data = " + data + ", " )
                    .append( " length( data ) = " + data.length )
                    .append( " }" ).toString();
        }
    }

    /**
     *
     */
    public static final class ControlMessage implements Serializable {

        private static final long serialVersionUID = 6852264669798022124L;

        public ControlMessage( final UUID messageID, final UUID srcTaskManagerID, Object data ) {
            // sanity check.
            if( messageID == null )
                throw new IllegalArgumentException( "messageID == null" );
            if( srcTaskManagerID == null )
                throw new IllegalArgumentException( "srcTaskManagerID == null" );
            if( data == null )
                throw new IllegalArgumentException( "data == null" );

            this.messageID = messageID;

            this.srcTaskManagerID = srcTaskManagerID;

            this.data = data;
        }

        public UUID messageID;

        public UUID srcTaskManagerID;

        public Object data;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append( "ControlMessage = {" )
                    .append( " messageID = " + messageID.toString() + ", " )
                    .append( " srcTaskID = " + srcTaskManagerID.toString() + ", " )
                    .append( " data = " + data + ", " )
                    .append( " }" ).toString();
        }
    }
}
