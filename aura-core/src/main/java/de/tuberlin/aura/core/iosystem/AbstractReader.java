package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.iosystem.buffer.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;

public abstract class AbstractReader implements IChannelReader {

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    protected final static Logger LOG = LoggerFactory.getLogger(AbstractReader.class);

    protected final SocketAddress socketAddress;
    protected final NioEventLoopGroup eventLoopGroup;
    private final int bufferSize;
    private final IEventDispatcher dispatcher;
    /**
     * Queues the single channels write their data into. There are possibly more channels that write into a single queue.
     * Each {@see de.tuberlin.aura.core.task.gates.InputGate} as exactly one queue.
     */
    private final List<BufferQueue<IOEvents.DataIOEvent>> queues;
    /**
     * In order to write the data to the correct queue, we map
     * (channel) -> queue index (in {@see queues})
     */
    private final Map<Channel, Integer> channelToQueueIndex;
    /**
     * In order to get the queue that belongs to the {@see InputGate}, we map
     * (task id, gate index) -> queue index (in {@see queues})
     */
    private final Map<Pair<UUID, Integer>, Integer> gateToQueueIndex;
    private final Map<Pair<UUID, Integer>, List<Channel>> connectedChannels;
    private ByteBufAllocator contextLevelAllocator;

    public AbstractReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, SocketAddress socketAddress) {
        this(dispatcher, eventLoopGroup, socketAddress, DEFAULT_BUFFER_SIZE);
    }

    public AbstractReader(IEventDispatcher dispatcher, NioEventLoopGroup eventLoopGroup, SocketAddress socketAddress, int bufferSize) {
        this.dispatcher = dispatcher;

        this.eventLoopGroup = eventLoopGroup;
        this.socketAddress = socketAddress;

        // TODO: need for concurrent queue?! after start we just read values?
        queues = new LinkedList<>();
        channelToQueueIndex = new HashMap<>();
        gateToQueueIndex = new HashMap<>();

        connectedChannels = new HashMap<>();

        if ((bufferSize & (bufferSize - 1)) != 0) {
            LOG.warn("The given buffer size is not a power of two.");
            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }
    }

    @Override
    public abstract void bind();

    @Override
    public BufferQueue<IOEvents.DataIOEvent> getInputQueue(final UUID taskID, final int gateIndex) {
        if (!gateToQueueIndex.containsKey(new Pair<UUID, Integer>(taskID, gateIndex))) {
            return null;
        }
        return queues.get(gateToQueueIndex.get(new Pair<UUID, Integer>(taskID, gateIndex)));
    }

    @Override
    public void setInputQueue(final UUID srcTaskID, final Channel channel, final int gateIndex, final BufferQueue<IOEvents.DataIOEvent> queue) {

        Pair<UUID, Integer> index = new Pair<>(srcTaskID, gateIndex);
        final int queueIndex;
        if (gateToQueueIndex.containsKey(index)) {
            queueIndex = gateToQueueIndex.get(index);
        } else {
            this.queues.add(queue);
            queueIndex = queues.size() - 1;
            gateToQueueIndex.put(index, queueIndex);
        }
        channelToQueueIndex.put(channel, queueIndex);

        final List<Channel> channels;
        if (!connectedChannels.containsKey(index)) {
            channels = new ArrayList<>();
        } else {
            channels = connectedChannels.get(index);
        }
        channels.add(channel);
        connectedChannels.put(index, channels);
    }

    @Override
    public void write(final UUID taskID, final int gateIndex, final int channelIndex, final IOEvents.DataIOEvent event) {
        connectedChannels.get(new Pair<UUID, Integer>(taskID, gateIndex)).get(channelIndex).writeAndFlush(event);
    }

    protected final class DataHandler extends SimpleChannelInboundHandler<IOEvents.DataBufferEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataBufferEvent event) {
            //event.setChannel(ctx.channel());

            LOG.info(ctx.channel() + " ----> " + event.toString());

            queues.get(channelToQueueIndex.get(ctx.channel())).offer(event);
        }
    }

    protected final class EventHandler extends SimpleChannelInboundHandler<IOEvents.DataIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.DataIOEvent event)
                throws Exception {

            //LOG.info(ctx.channel() + " ----> " + event.toString());

            //LOG.info("got event: " + event.type);
            if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED)) {

                // TODO: ensure that queue is bound before first data buffer event arrives

                IOEvents.GenericIOEvent connected =
                        new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, AbstractReader.this, event.srcTaskID, event.dstTaskID);
                connected.setChannel(ctx.channel());
                dispatcher.dispatchEvent(connected);
            } else if (event.type.equals(IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED)) {
                queues.get(channelToQueueIndex.get(ctx.channel())).offer(event);
            } else {
                event.setChannel(ctx.channel());
                dispatcher.dispatchEvent(event);
            }
        }
    }

    // TODO: Make use of flip buffer
    // TODO see FixedLengthFrameDecoder for more error resistant solution
    // TODO use the context lvl buffer provider here instead of i/o lvl
    private class MergeBufferInHandler extends ChannelInboundHandlerAdapter {
        private ByteBuf buf;

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            buf = contextLevelAllocator.buffer(bufferSize, bufferSize);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            buf.release();
            buf = null;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg;
            // do not overflow the destination buffer
            buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));

            while (buf.readableBytes() >= bufferSize) {
                ctx.fireChannelRead(buf);
                buf.clear();

                // add rest of buffer to dest
                // sadly this is not true // guaranteed to not overflow, if we set the rcv_buffer size to buffer size
                //assert m.writableBytes() <= bufferSize;
                buf.writeBytes(m, Math.min(m.readableBytes(), bufferSize - buf.readableBytes()));
            }
            m.release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // TODO: proper exception handling
            cause.printStackTrace();
            ctx.close();
        }
    }
}
