package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.UUID;

public class LocalChannelWriter extends AbstractWriter {

    public LocalChannelWriter(final UUID srcTaskID, final UUID dstTaskID, IEventDispatcher dispatcher, LocalAddress remoteAddress, EventLoopGroup eventLoopGroup) {
        this(srcTaskID, dstTaskID, dispatcher, remoteAddress, eventLoopGroup, DEFAULT_BUFFER_SIZE);
    }

    public LocalChannelWriter(final UUID srcTaskID, final UUID dstTaskID, IEventDispatcher dispatcher, LocalAddress remoteAddress, EventLoopGroup eventLoopGroup, int bufferSize) {

        super(srcTaskID, dstTaskID, dispatcher, remoteAddress, eventLoopGroup, bufferSize);
    }

    @Override
    public void connect() {
        Bootstrap bs = new Bootstrap();
        bs.group(eventLoopGroup)
                .channel(LocalChannel.class)
                        // the mark the outbound buffer has to reach in order to change the writable state of a channel true
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
                        // the mark the outbound buffer has to reach in order to change the writable state of a channel false
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize)
                        // dummy allocator here, as we do not invoke the allocator in the event loop
                        //.option(ChannelOption.ALLOCATOR, new FlipBufferAllocator(bufferSize, 1, true))
                        // set the channelWritable spin lock
                .handler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new WritableHandler())
                                .addLast(new ObjectEncoder());
                    }
                });

        // on success:
        // the close future is registered
        // the polling thread is started
        bs.connect(remoteAddress).addListener(new ConnectListener());
    }
}
