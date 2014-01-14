package de.tuberlin.aura.core.task.gates;

import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.common.TaskContext;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractGate {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractGate(final TaskContext context, int gateIndex, int numChannels) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        this.context = context;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;

        if (numChannels > 0) {
            bufferQueues = new ArrayList<BufferQueue<IOEvents.DataIOEvent>>(Collections.nCopies(numChannels, (BufferQueue<IOEvents.DataIOEvent>) null));
            channels = new ArrayList<Channel>(Collections.nCopies(numChannels, (Channel) null));
        } else { // numChannels == 0
            bufferQueues = null;
            channels = null;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TaskContext context;

    protected final int numChannels;

    protected final int gateIndex;

    protected final List<BufferQueue<IOEvents.DataIOEvent>> bufferQueues;

    protected final List<Channel> channels;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void setQueue(int channelIndex, final BufferQueue<IOEvents.DataIOEvent> queue) {
        // sanity check.
        if (channelIndex < 0)
            throw new IllegalArgumentException("channelIndex < 0");
        if (channelIndex >= numChannels)
            throw new IllegalArgumentException("channelIndex >= numChannels");
        if (bufferQueues == null)
            throw new IllegalStateException("bufferQueues == null");

        bufferQueues.set(channelIndex, queue);
    }

    public BufferQueue<IOEvents.DataIOEvent> getQueue(int channelIndex) {
        // sanity check.
        if (channelIndex < 0)
            throw new IllegalArgumentException("channelIndex < 0");
        if (channelIndex >= numChannels)
            throw new IllegalArgumentException("channelIndex >= numChannels");
        if (bufferQueues == null)
            throw new IllegalStateException("bufferQueues == null");

        return bufferQueues.get(channelIndex);
    }

    public void setChannel(int channelIndex, final Channel channel) {
        // sanity check.
        if (channelIndex < 0)
            throw new IllegalArgumentException("channelIndex < 0");
        if (channelIndex >= numChannels)
            throw new IllegalArgumentException("channelIndex >= numChannels");
        if (channels == null)
            throw new IllegalStateException("channels == null");

        channels.set(channelIndex, channel);
    }

    public Channel getChannel(int channelIndex) {
        // sanity check.
        if (channelIndex < 0)
            throw new IllegalArgumentException("channelIndex < 0");
        if (channelIndex >= numChannels)
            throw new IllegalArgumentException("channelIndex >= numChannels");
        if (channels == null)
            throw new IllegalStateException("channels == null");

        return channels.get(channelIndex);
    }

    public List<Channel> getAllChannels() {
        return Collections.unmodifiableList(channels);
    }
}
