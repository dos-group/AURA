package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOMessages.DataChannelGateMessage;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;

public final class InputGate extends AbstractGate {

    public InputGate( final UUID taskID, final List<TaskDescriptor> gateBinding ) {
        super( taskID, gateBinding );

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

    public void openGate() {
        for( int i = 0; i < numChannels; ++i ) {
            final Channel ch = channels.get( i );
            final UUID srcID = gateBinding.get( i ).uid;
            ch.writeAndFlush( new DataChannelGateMessage( srcID, taskID,
                    DataChannelGateMessage.DATA_CHANNEL_OUTPUT_GATE_OPEN ) );
        }
    }

    public void closeGate() {
        for( int i = 0; i < numChannels; ++i ) {
            final Channel ch = channels.get( i );
            final UUID srcID = gateBinding.get( i ).uid;
            ch.writeAndFlush( new DataChannelGateMessage( srcID, taskID,
                    DataChannelGateMessage.DATA_CHANNEL_OUTPUT_GATE_CLOSE ) );
        }
    }
}
