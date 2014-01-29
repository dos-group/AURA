package de.tuberlin.aura.core.iosystem;

import java.net.SocketAddress;
import java.util.UUID;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

public interface IChannelReader {

    BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex);

    void setInputQueue(final UUID srcTaskID, final Channel channel, final int gateIndex, final BufferQueue<IOEvents.DataIOEvent> queue);

    void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event);

    <T extends Channel> void bind(final InputReader.ConnectionType type, final SocketAddress address, final EventLoopGroup workerGroup);

    void connectedChannels(UUID taskID, int gateIndex, int channelIndex);

    boolean isConnected(UUID taskID, int gateIndex, int channelIndex);
}
