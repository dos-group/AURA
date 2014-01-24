package de.tuberlin.aura.core.iosystem;

import io.netty.channel.Channel;

import java.util.UUID;

public interface IChannelReader {

    BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex);

    void setInputQueue(final UUID srcTaskID, final Channel channel, final int gateIndex, final BufferQueue<IOEvents.DataIOEvent> queue);

    void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event);

    void bind();
}
