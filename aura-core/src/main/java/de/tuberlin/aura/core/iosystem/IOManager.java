package de.tuberlin.aura.core.iosystem;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlIOEvent;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.task.common.TaskExecutionManager;
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

public final class IOManager extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(IOManager.class);

    public final MachineDescriptor machine;

    private final Map<Pair<UUID, UUID>, Channel> controlIOConnections;

    private final ChannelBuilder channelBuilder;

    private final LocalAddress localAddress = new LocalAddress(UUID.randomUUID().toString());

    private final EventHandler controlEventHandler;

    private final DataReader dataReader;

    private final DataWriter dataWriter;

    // Event Loops for Netty
    private final NioEventLoopGroup controlPlaneEventLoopGroup;

    private final NioEventLoopGroup networkConnectionListenerEventLoopGroup;

    private final LocalEventLoopGroup localConnectionListenerEventLoopGroup;


    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public IOManager(final MachineDescriptor machine, final TaskExecutionManager executionManager) {

        // Event dispatcher doesn't use an own thread.
        super();

        // sanity check.
        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        this.machine = machine;

        this.controlIOConnections = new ConcurrentHashMap<>();

        this.channelBuilder = new ChannelBuilder();

        this.dataReader = new DataReader(IOManager.this, executionManager);

        this.dataWriter = new DataWriter(IOManager.this);

        // TODO: Make the number of thread configurable
        this.networkConnectionListenerEventLoopGroup = new NioEventLoopGroup(4);
        this.localConnectionListenerEventLoopGroup = new LocalEventLoopGroup(4);

        startNetworkConnectionSetupServer(this.machine, networkConnectionListenerEventLoopGroup);

        startLocalDataConnectionSetupServer(localConnectionListenerEventLoopGroup);

        // Configure the control plane.
        this.controlPlaneEventLoopGroup = new NioEventLoopGroup();

        startNetworkControlMessageServer(this.machine, controlPlaneEventLoopGroup);

        this.controlEventHandler = new ControlIOChannelEventHandler();

        this.addEventListener(ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED, controlEventHandler);

        this.addEventListener(ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, controlEventHandler);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param srcTaskID
     * @param dstTaskID
     * @param dstMachine
     */
    public void connectDataChannel(final UUID srcTaskID,
                                   final UUID dstTaskID,
                                   final MachineDescriptor dstMachine,
                                   final MemoryManager.Allocator allocator) {
        // sanity check.
        if (srcTaskID == null)
            throw new IllegalArgumentException("srcTask == null");
        if (dstTaskID == null)
            throw new IllegalArgumentException("dstTask == null");
        if (dstMachine == null)
            throw new IllegalArgumentException("dstTask == null");
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");

        if (machine.equals(dstMachine)) {
            channelBuilder.buildLocalDataChannel(srcTaskID, dstTaskID, allocator);
        } else {
            channelBuilder.buildNetworkDataChannel(srcTaskID, dstTaskID, dstMachine.dataAddress, allocator);
        }
    }

    /**
     * @param srcTaskID
     * @param dstTaskID
     * @param dstMachine
     */
    public void disconnectDataChannel(final UUID srcTaskID, final UUID dstTaskID, final MachineDescriptor dstMachine) {
        // sanity check.
        if (srcTaskID == null)
            throw new IllegalArgumentException("srcTask == null");
        if (dstTaskID == null)
            throw new IllegalArgumentException("dstTask == null");
        if (dstMachine == null)
            throw new IllegalArgumentException("dstTask == null");

        // TODO: implement it!
    }

    /**
     * @param dstMachine
     */
    public void connectMessageChannelBlocking(final MachineDescriptor dstMachine) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("dstMachine == null");
        if (machine.controlAddress.equals(dstMachine.controlAddress))
            throw new IllegalArgumentException("can not setup message channel");

        // If connection already exists, we return.
        if (controlIOConnections.get(new Pair<>(machine.uid, dstMachine.uid)) != null) {
            LOG.info("connection already established");
            return;
        }

        // TODO: brrrr, use something different to make the call blocking until the channel is
        // established!
        // Maybe a design with a Future?

        final Lock threadLock = new ReentrantLock();
        final Condition condition = threadLock.newCondition();

        // TODO: that´s bullshit, we make here assumptions on the evaluation order of the event
        // handlers. Against the general event contract!
        final IEventHandler localHandler = new IEventHandler() {

            // runs in the channel thread...
            @Override
            public void handleEvent(Event e) {
                if (e instanceof ControlIOEvent) {
                    final ControlIOEvent event = (ControlIOEvent) e;
                    if (dstMachine.uid.equals(event.getDstMachineID())) {
                        threadLock.lock();
                        condition.signal();
                        threadLock.unlock();
                    }
                }
            }
        };

        addEventListener(ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler);
        channelBuilder.buildNetworkControlChannel(machine.uid, dstMachine.uid, dstMachine.controlAddress);

        threadLock.lock();
        try {
            condition.await();
        } catch (InterruptedException e) {
            LOG.info("Condition interrupted", e);
        } finally {
            threadLock.unlock();
        }

        removeEventListener(ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, localHandler);
    }

    /**
     * @param dstMachine
     * @return
     */
    public Channel getControlIOChannel(final MachineDescriptor dstMachine) {
        // sanity check.
        if (machine == null)
            throw new IllegalArgumentException("machine == null");

        return controlIOConnections.get(new Pair<>(machine.uid, dstMachine.uid));
    }

    /**
     * @param dstMachine
     * @param event
     */
    public void sendEvent(final MachineDescriptor dstMachine, final ControlIOEvent event) {
        // sanity check.
        if (dstMachine == null)
            throw new IllegalArgumentException("machine == null");

        sendEvent(dstMachine.uid, event);
    }

    /**
     * @param dstMachineID
     * @param event
     */
    public void sendEvent(final UUID dstMachineID, final ControlIOEvent event) {

        // sanity check.
        if (dstMachineID == null)
            throw new IllegalArgumentException("machine == null");
        if (event == null)
            throw new IllegalArgumentException("event == null");

        final Channel channel = controlIOConnections.get(new Pair<>(machine.uid, dstMachineID));

        if (channel == null) {
            throw new IllegalStateException("channel is not registered");
        }

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

    /**
     * @param machine
     * @param nelg
     */
    private void startNetworkConnectionSetupServer(final MachineDescriptor machine, final NioEventLoopGroup nelg) {
        dataReader.bind(new DataReader.NetworkConnection(), machine.dataAddress, nelg);
    }

    /**
     * @param lelg
     */
    private void startLocalDataConnectionSetupServer(final LocalEventLoopGroup lelg) {
        dataReader.bind(new DataReader.LocalConnection(), localAddress, lelg);
    }

    /**
     * @param machine
     * @param nelg
     */
    private void startNetworkControlMessageServer(final MachineDescriptor machine, final NioEventLoopGroup nelg) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(nelg).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {

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

    private final class ControlIOChannelHandler extends SimpleChannelInboundHandler<ControlIOEvent> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final ControlIOEvent event) throws Exception {
            event.setChannel(ctx.channel());
            dispatchEvent(event);
        }
    }

    /**
     *
     */
    private final class ControlIOChannelEventHandler extends EventHandler {

        @Handle(event = ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED)
        private void handleControlChannelInputConnected(final ControlIOEvent event) {
            controlIOConnections.put(new Pair<>(machine.uid, event.getSrcMachineID()), event.getChannel());
            LOG.info("CONTROL I/O CONNECTION BETWEEN MACHINE " + event.getSrcMachineID() + " AND " + event.getDstMachineID() + " ESTABLISHED");
        }

        @Handle(event = ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED)
        private void handleControlChannelOutputConnected(final ControlIOEvent event) {
            controlIOConnections.put(new Pair<>(machine.uid, event.getDstMachineID()), event.getChannel());
            LOG.info("CONTROL I/O CONNECTION BETWEEN MACHINE " + event.getSrcMachineID() + " AND " + event.getDstMachineID() + " ESTABLISHED");
        }
    }

    /**
     *
     */
    private final class ChannelBuilder {

        public void buildNetworkDataChannel(final UUID srcTaskID,
                                            final UUID dstTaskID,
                                            final InetSocketAddress socketAddress,
                                            final MemoryManager.Allocator allocator) {
            // sanity check.
            if (srcTaskID == null)
                throw new IllegalArgumentException("srcTaskID == null");
            if (dstTaskID == null)
                throw new IllegalArgumentException("dstTaskID == null");
            if (socketAddress == null)
                throw new IllegalArgumentException("socketAddress == null");
            if (allocator == null)
                throw new IllegalArgumentException("allocator == null");

            dataWriter.bind(srcTaskID, dstTaskID, new AbstractConnection.NetworkConnection(), socketAddress, allocator);
        }

        public void buildLocalDataChannel(final UUID srcTaskID, final UUID dstTaskID, final MemoryManager.Allocator allocator) {
            // sanity check.
            if (srcTaskID == null)
                throw new IllegalArgumentException("srcTaskID == null");
            if (dstTaskID == null)
                throw new IllegalArgumentException("dstTaskID == null");
            if (allocator == null)
                throw new IllegalArgumentException("allocator == null");

            dataWriter.bind(srcTaskID, dstTaskID, new AbstractConnection.LocalConnection(), localAddress, allocator);
        }

        public void buildNetworkControlChannel(final UUID srcMachineID, final UUID dstMachineID, final InetSocketAddress socketAddress) {
            // sanity check.
            if (socketAddress == null)
                throw new IllegalArgumentException("socketAddress == null");

            final Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(controlPlaneEventLoopGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addFirst(new ObjectEncoder());
                    ch.pipeline().addFirst(new ControlIOChannelHandler());
                    ch.pipeline().addFirst(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())));
                }
            });

            final ChannelFuture cf = bootstrap.connect(socketAddress);
            cf.addListener(new ChannelFutureListener() {

                @Override
                public void operationComplete(ChannelFuture cf) throws Exception {

                    if (cf.isSuccess()) {

                        cf.channel().writeAndFlush(new ControlIOEvent(ControlEventType.CONTROL_EVENT_INPUT_CHANNEL_CONNECTED,
                                                                      srcMachineID,
                                                                      dstMachineID));

                        final ControlIOEvent event =
                                new ControlIOEvent(ControlEventType.CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED, srcMachineID, dstMachineID);

                        event.setChannel(cf.channel());
                        dispatchEvent(event);

                    } else {
                        LOG.error("connection attempt failed: " + cf.cause().getLocalizedMessage());
                    }
                }
            });
        }
    }
}
