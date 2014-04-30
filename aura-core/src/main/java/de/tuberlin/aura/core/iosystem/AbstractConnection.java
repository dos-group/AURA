package de.tuberlin.aura.core.iosystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public abstract class AbstractConnection<T> implements OutgoingConnectionType<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DataWriter.class);

    public static final int DEFAULT_BUFFER_SIZE = 64 << 10; // 65536

    protected final int bufferSize;

    public AbstractConnection(int bufferSize) {
        if ((bufferSize & (bufferSize - 1)) != 0) {
            // not a power of two
            LOG.warn("The given buffer size is not a power of two.");

            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }
    }

    public AbstractConnection() {
        this(DEFAULT_BUFFER_SIZE);
    }

    public static class LocalConnection extends AbstractConnection<LocalChannel> {

        public LocalConnection() {
            super();
        }

        public LocalConnection(int bufferSize) {
            super(bufferSize);
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(LocalChannel.class)
            // the mark the outbound bufferQueue has to reach in order
            // to change the writable state of a channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order
              // to change the writable state of a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }

    public static class NetworkConnection extends AbstractConnection<SocketChannel> {

        public NetworkConnection() {
            super();
        }

        public NetworkConnection(int bufferSize) {
            super(bufferSize);
        }

        @Override
        public Bootstrap bootStrap(EventLoopGroup eventLoopGroup) {
            Bootstrap bs = new Bootstrap();
            bs.group(eventLoopGroup).channel(NioSocketChannel.class)
            // true, periodically heartbeats from tcp
              .option(ChannelOption.SO_KEEPALIVE, true)
              // false, means that messages get only sent if the size of the data reached a relevant
              // amount.
              .option(ChannelOption.TCP_NODELAY, false)
              // size of the system lvl send bufferQueue PER SOCKET
              // -> bufferQueue size, as we always have only 1 channel per socket in the examples
              // case
              .option(ChannelOption.SO_SNDBUF, bufferSize)
              // the mark the outbound bufferQueue has to reach in order to change the writable
              // state of a channel true
              .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 0)
              // the mark the outbound bufferQueue has to reach in order to change the writable
              // state of a channel false
              .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, bufferSize);

            return bs;
        }
    }
}
