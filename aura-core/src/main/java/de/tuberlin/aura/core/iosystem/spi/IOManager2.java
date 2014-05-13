package de.tuberlin.aura.core.iosystem.spi;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.tuberlin.aura.core.memory.spi.IAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 *
 */
public class IOManager2 extends EventDispatcher implements IIOManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(IOManager2.class);

    public final Descriptors.MachineDescriptor machine;

    private final Map<Pair<UUID, UUID>, Channel> controlChannels;

    private final LocalAddress localAddress;


    private final DataReader dataReader;

    private final DataWriter dataWriter;

    // Netty Event Loops.

    private final NioEventLoopGroup     controlELG;

    private final NioEventLoopGroup     networkInputListenerELG;

    private final LocalEventLoopGroup   localInputListenerELG;

    private final NioEventLoopGroup     networkOutputListenerELG;

    private final LocalEventLoopGroup   localOutputListenerELG;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public IOManager2(final Descriptors.MachineDescriptor machine) {

        super(false);

        // sanity check.
        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        this.machine = machine;

        this.controlChannels = new ConcurrentHashMap<>();

        this.localAddress = new LocalAddress(UUID.randomUUID().toString());


        this.dataReader = new DataReader(this, null);

        this.dataWriter = new DataWriter(this);


        // Configure netty Event Loops.

        this.controlELG = new NioEventLoopGroup();

        networkInputListenerELG = new NioEventLoopGroup(4);

        localInputListenerELG = new LocalEventLoopGroup(4);

        networkOutputListenerELG = new NioEventLoopGroup(4);

        localOutputListenerELG = new LocalEventLoopGroup(4);


        // Start Channel Listener.

        startNetworkDataChannelListener(this.machine, networkInputListenerELG);

        startLocalDataChannelListener(localInputListenerELG);


        // Setup Control Channel Server.

        startNetworkControlChannelServer(this.machine, controlELG);

        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                if(e instanceof IOEvents.ControlIOEvent) {
                    final IOEvents.ControlIOEvent event = (IOEvents.ControlIOEvent)e;
                    controlChannels.put(new Pair<>(machine.uid, event.getSrcMachineID()), event.getChannel());
                    LOG.info("CONTROL I/O CONNECTION BETWEEN MACHINE " + event.getSrcMachineID() + " AND " + event.getDstMachineID() + " ESTABLISHED");
                }
            }
        });

        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                if(e instanceof IOEvents.ControlIOEvent) {
                    final IOEvents.ControlIOEvent event = (IOEvents.ControlIOEvent)e;
                    controlChannels.put(new Pair<>(machine.uid, event.getDstMachineID()), event.getChannel());
                    LOG.info("CONTROL I/O CONNECTION BETWEEN MACHINE " + event.getSrcMachineID() + " AND " + event.getDstMachineID() + " ESTABLISHED");
                }
            }
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void connectDataChannel(Descriptors.MachineDescriptor dstMachine, UUID srcTaskID, UUID dstTaskID, IAllocator allocator) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("dstMachine == null");
        if (srcTaskID == null)
            throw new IllegalArgumentException("srcTask == null");
        if (dstTaskID == null)
            throw new IllegalArgumentException("dstTask == null");
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");

        if (machine.equals(dstMachine)) {
            buildLocalDataChannel(srcTaskID, dstTaskID, allocator);
        } else {
            buildNetworkDataChannel(srcTaskID, dstTaskID, dstMachine.dataAddress, allocator);
        }
    }

    @Override
    public Channel getDataChannel(UUID srcTaskID, UUID dstTaskID) {
        throw new NotImplementedException();
    }

    @Override
    public void disconnectDataChannel(UUID srcTaskID, UUID dstTaskID) {
        // sanity check.
        if (srcTaskID == null)
            throw new IllegalArgumentException("srcTask == null");
        if (dstTaskID == null)
            throw new IllegalArgumentException("dstTask == null");

        throw new NotImplementedException();
    }


    @Override
    public void connectControlChannel(final Descriptors.MachineDescriptor dstMachine) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("dstMachine == null");
        if (machine.controlAddress.equals(dstMachine.controlAddress))
            throw new IllegalArgumentException("can not setup message channel");

        // If connection already exists, we return.
        if (controlChannels.get(new Pair<>(machine.uid, dstMachine.uid)) != null) {
            LOG.info("connection already established");
            return;
        }

        // TODO: brrrr, use something different to make the call blocking until the channel is established!
        // Maybe a design with a Future?

        final Lock threadLock = new ReentrantLock();
        final Condition condition = threadLock.newCondition();

        // TODO: that´s bullshit, we make here assumptions on the evaluation order of the event
        // handlers. Against the general event contract!
        final IEventHandler localHandler = new IEventHandler() {

            // runs in the channel thread...
            @Override
            public void handleEvent(Event e) {
                if (e instanceof IOEvents.ControlIOEvent) {
                    final IOEvents.ControlIOEvent event = (IOEvents.ControlIOEvent) e;
                    if (dstMachine.uid.equals(event.getDstMachineID())) {
                        threadLock.lock();
                        condition.signal();
                        threadLock.unlock();
                    }
                }
            }
        };

        addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler);
        buildNetworkControlChannel(dstMachine.controlAddress, machine.uid, dstMachine.uid);

        threadLock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            LOG.info("Condition interrupted", e);
        } finally {
            threadLock.unlock();
        }

        removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler);
    }

    @Override
    public Channel getControlChannel(Descriptors.MachineDescriptor dstMachine) {
        // sanity check.
        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        return controlChannels.get(new Pair<>(machine.uid, dstMachine.uid));
    }

    @Override
    public void disconnectControlChannel(Descriptors.MachineDescriptor dstMachine) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("dstMachine == null");

        throw new NotImplementedException();
    }

    @Override
    public void sendControlEvent(Descriptors.MachineDescriptor dstMachine, IOEvents.ControlIOEvent event) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("machine == null");

        sendControlEvent(dstMachine.uid, event);
    }

    @Override
    public void sendControlEvent(UUID dstMachineID, IOEvents.ControlIOEvent event) {
        // sanity check.
        if (dstMachineID == null)
            throw new IllegalArgumentException("machine == null");
        if (event == null)
            throw new IllegalArgumentException("event == null");

        final Channel channel = controlChannels.get(new Pair<>(machine.uid, dstMachineID));

        if (channel == null)
            throw new IllegalStateException("channel is not registered");

        event.setSrcMachineID(machine.uid);
        event.setDstMachineID(dstMachineID);

        try {
            channel.writeAndFlush(event).sync();
        } catch (InterruptedException e) {
            LOG.error("Write interrupted", e);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    public void buildNetworkDataChannel(final UUID srcTaskID,
                                        final UUID dstTaskID,
                                        final InetSocketAddress socketAddress,
                                        final IAllocator allocator) {

        dataWriter.bind(srcTaskID, dstTaskID, new DataWriter.NetworkConnection(), socketAddress, networkOutputListenerELG, allocator);
    }

    public void buildLocalDataChannel(final UUID srcTaskID, final UUID dstTaskID, final IAllocator allocator) {

        dataWriter.bind(srcTaskID, dstTaskID, new DataWriter.LocalConnection(), localAddress, localOutputListenerELG, allocator);
    }

    public void buildNetworkControlChannel(final InetSocketAddress dstSocketAddress, final UUID srcMachineID, final UUID dstMachineID) {
        // sanity check.
        if (dstSocketAddress == null)
            throw new IllegalArgumentException("dstSocketAddress == null");
        if (srcMachineID == null)
            throw new IllegalArgumentException("srcMachineID == null");
        if (dstMachineID == null)
            throw new IllegalArgumentException("dstMachineID == null");

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(controlELG).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addFirst(new ObjectEncoder());
                ch.pipeline().addFirst(new ControlIOChannelHandler());
                ch.pipeline().addFirst(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
            }
        });

        final ChannelFuture cf = bootstrap.connect(dstSocketAddress);
        cf.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture cf) throws Exception {

                if (cf.isSuccess()) {

                    cf.channel().writeAndFlush(
                            new IOEvents.ControlIOEvent(
                                    IOEvents.ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED,
                                    srcMachineID,
                                    dstMachineID
                            )
                    );

                    final IOEvents.ControlIOEvent event =
                            new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, srcMachineID, dstMachineID);

                    event.setChannel(cf.channel());
                    dispatchEvent(event);

                } else {
                    LOG.error("connection attempt failed: " + cf.cause().getLocalizedMessage());
                }
            }
        });
    }

    /**
     * @param machine
     * @param eventLoopGroup
     */
    private void startNetworkDataChannelListener(final Descriptors.MachineDescriptor machine, final NioEventLoopGroup eventLoopGroup) {
        dataReader.bind(new DataReader.NetworkConnection(), machine.dataAddress, eventLoopGroup);
    }

    /**
     * @param eventLoopGroup
     */
    private void startLocalDataChannelListener(final LocalEventLoopGroup eventLoopGroup) {
        dataReader.bind(new DataReader.LocalConnection(), localAddress, eventLoopGroup);
    }

    /**
     * @param machine
     * @param eventLoopGroup
     */
    private void startNetworkControlChannelServer(final Descriptors.MachineDescriptor machine, final NioEventLoopGroup eventLoopGroup) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(eventLoopGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addFirst(new ObjectEncoder());
                ch.pipeline().addFirst(new ControlIOChannelHandler());
                ch.pipeline().addFirst(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
            }
        });

        final ChannelFuture cf = bootstrap.bind(machine.controlAddress);
        cf.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (cf.isSuccess()) {
                    LOG.info("network server bound to address " + machine.dataAddress);
                } else {
                    LOG.error("bound attempt failed: " + cf.cause().getLocalizedMessage());
                    throw new IllegalStateException("could not start netty network server");
                }
            }
        });

        // Wait until the netty-server is bound.
        try {
            cf.sync();
        } catch (InterruptedException e) {
            LOG.error("Waiting for netty bound was interrupted", e);
        }
    }


    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    private final class ControlIOChannelHandler extends SimpleChannelInboundHandler<IOEvents.ControlIOEvent> {
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final IOEvents.ControlIOEvent event) throws Exception {
            event.setChannel(ctx.channel());
            dispatchEvent(event);
        }
    }
}
