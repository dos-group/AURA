package de.tuberlin.aura.core.task.common;


import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.utils.NettyHelper;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.*;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitLocalInputEventLoopGroup;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitNetworkInputEventLoopGroup;
import de.tuberlin.aura.core.memory.MemoryManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;

public final class TaskExecutionManager extends EventDispatcher {

    // ---------------------------------------------------
    // Execution Manager Events.
    // ---------------------------------------------------

    /**
     *
     */
    public static final class TaskExecutionEvent extends Event {

        public static final String EXECUTION_MANAGER_EVENT_UNREGISTER_TASK = "EXECUTION_MANAGER_EVENT_UNREGISTER_TASK";

        public TaskExecutionEvent(String type, Object payload) {
            super(type, payload);
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionManager.class);

    private final Descriptors.MachineDescriptor machineDescriptor;

    private final int numberOfCores;

    private final TaskExecutionUnit[] executionUnit;

    private final MemoryManager.BufferMemoryManager bufferMemoryManager;

    private IOManager ioManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param machineDescriptor
     * @param bufferMemoryManager
     */
    public TaskExecutionManager(final Descriptors.MachineDescriptor machineDescriptor, final MemoryManager.BufferMemoryManager bufferMemoryManager) {
        // TODO: Cleanup
        super(true, "TaskExecutionManagerEventDispatcher");

        // sanity check.
        if (machineDescriptor == null)
            throw new IllegalArgumentException("machineDescriptor == null");
        if (bufferMemoryManager == null)
            throw new IllegalArgumentException("bufferMemoryManager == null");

        this.machineDescriptor = machineDescriptor;

        this.bufferMemoryManager = bufferMemoryManager;

        this.numberOfCores = machineDescriptor.hardware.cpuCores;

        this.executionUnit = new TaskExecutionUnit[numberOfCores];

        initializeExecutionUnits();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param driverContext
     */
    public void scheduleTask(final TaskDriverContext driverContext) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");

        int tmpMin, tmpMinOld;
        tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
        int selectedEU = 0;

        for (int i = 1; i < numberOfCores; ++i) {
            tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
            if (tmpMin < tmpMinOld) {
                tmpMinOld = tmpMin;
                selectedEU = i;
            }
        }

        driverContext.setAssignedExecutionUnitIndex(selectedEU);
        executionUnit[selectedEU].enqueueTask(driverContext);

        LOG.info("EXECUTE TASK {}-{} [{}] ON EXECUTION UNIT ({}) ON MACHINE [{}]",
                 driverContext.taskDescriptor.name,
                 driverContext.taskDescriptor.taskIndex,
                 driverContext.taskDescriptor.taskID,
                 executionUnit[selectedEU].getExecutionUnitID(),
                 machineDescriptor.uid);
    }

    /**
     * @param taskID
     * @return
     */
    public TaskExecutionUnit findTaskExecutionUnitByTaskID(final UUID taskID) {
        // sanity check.
        if (taskID == null)
            throw new IllegalArgumentException("taskID == null");

        for (int i = 0; i < numberOfCores; ++i) {
            final TaskExecutionUnit eu = executionUnit[i];
            final TaskDriverContext taskCtx = eu.getCurrentTaskDriverContext();
            if (taskCtx != null && taskID.equals(taskCtx.taskDescriptor.taskID)) {
                return eu;
            }
        }

        LOG.trace("No task execution unit was found for this task ID: {}", taskID);
        return null;
    }

    public void setIOManager(IOManager ioManager) {
        this.ioManager = ioManager;

        registerEventListeners();
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     *
     */
    private void initializeExecutionUnits() {
        for (int i = 0; i < numberOfCores; ++i) {
            final MemoryManager.BufferAllocatorGroup inputBuffer = bufferMemoryManager.getBufferAllocatorGroup();
            final MemoryManager.BufferAllocatorGroup outputBuffer = bufferMemoryManager.getBufferAllocatorGroup();

            this.executionUnit[i] = new TaskExecutionUnit(this, i, inputBuffer, outputBuffer);
            this.executionUnit[i].start();
        }
    }

    /**
     * Register event listeners to the IOManager.
     */
    private void registerEventListeners() {
        this.ioManager.addEventListener(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_SETUP, new EventHandler() {

            private ExecutorService executor = Executors.newSingleThreadExecutor();

            @SuppressWarnings("deprecation")
            @EventHandler.Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_SETUP)
            private void handleInputChannelSetup(final IOEvents.DataIOEvent event) {

                try {
                    // Add the channel to the according event loop group.
                    final Channel channel = event.getChannel();

                    // TODO: Dirty dirty hack... set channel to state "closed" to avoid closing the
                    // peer
                    if (channel instanceof LocalChannel) {
                        Class<?> clazz = channel.getClass();
                        Field stateField = clazz.getDeclaredField("state");
                        stateField.setAccessible(true);
                        stateField.setInt(channel, 3);
                    }

                    channel.deregister().addListener(new ChannelFutureListener() {

                        @Override
                        public void operationComplete(final ChannelFuture future) throws Exception {

                            channel.eventLoop().execute(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        Channel channel = future.channel();

                                        if (channel.pipeline().names().size() == 1) {
                                            // Set the pipeline again.
                                            DataReader dataReader = (DataReader) event.getPayload();
                                            channel.pipeline()
                                                   .addLast(NettyHelper.getLengthDecoder())
                                                   .addLast(new KryoEventSerializer.KryoOutboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(null,
                                                                                                                                                              TaskExecutionManager.this)))
                                                   .addLast(new KryoEventSerializer.KryoInboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(null,
                                                                                                                                                             TaskExecutionManager.this)))
                                                   .addLast(dataReader.new DataHandler())
                                                   .addLast(dataReader.new EventHandler());
                                        }

                                        // Determine the execution unit the given channel is
                                        // connected to.
                                        TaskExecutionUnit executionUnit = null;
                                        while ((executionUnit = findTaskExecutionUnitByTaskID(event.dstTaskID)) == null) {
                                            Thread.sleep(100);
                                        }

                                        LOG.trace("Change event loop from {} to event loop of task {}", channel.eventLoop().parent(), event.dstTaskID);

                                        if (channel instanceof LocalChannel) {
                                            ExecutionUnitLocalInputEventLoopGroup eventLoopGroup =
                                                    executionUnit.dataFlowEventLoops.localInputEventLoopGroup;

                                            // TODO: Dirty dirty hack... [will be fixed in Netty
                                            // 4.0.19.Final]
                                            Class<?> clazz = channel.getClass();
                                            Field stateField = clazz.getDeclaredField("state");
                                            stateField.setAccessible(true);
                                            stateField.setInt(channel, 0);

                                            Field peerField = clazz.getDeclaredField("peer");
                                            peerField.setAccessible(true);
                                            LocalChannel peer = (LocalChannel) peerField.get(channel);

                                            Field connectPromiseField = clazz.getDeclaredField("connectPromise");
                                            connectPromiseField.setAccessible(true);
                                            connectPromiseField.set(peer, peer.unsafe().voidPromise());

                                            // Change event loop group.
                                            eventLoopGroup.register(channel,
                                                                    event.srcTaskID,
                                                                    executionUnit.getCurrentTaskDriverContext().taskBindingDescriptor.inputGateBindings)
                                                          .sync();
                                        } else {
                                            ExecutionUnitNetworkInputEventLoopGroup eventLoopGroup =
                                                    executionUnit.dataFlowEventLoops.networkInputEventLoopGroup;

                                            // Change event loop group.
                                            eventLoopGroup.register(channel,
                                                                    event.srcTaskID,
                                                                    executionUnit.getCurrentTaskDriverContext().taskBindingDescriptor.inputGateBindings)
                                                          .sync();
                                        }

                                        LOG.trace("Changed event loop to {}", channel.eventLoop().parent());
                                    } catch (IllegalAccessException | NoSuchFieldException | InterruptedException e) {
                                        LOG.error(e.getLocalizedMessage(), e);
                                    }


                                    // Enable auto read again.
                                    channel.config().setAutoRead(true);

                                    // Dispatch INPUT_CHANNEL_CONNECTED event.
                                    IOEvents.GenericIOEvent connected =
                                            new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                                                        event.getPayload(),
                                                                        event.srcTaskID,
                                                                        event.dstTaskID,
                                                                        true);
                                    connected.setChannel(event.getChannel());
                                    ioManager.dispatchEvent(connected);
                                }
                            });
                        }
                    })
                           .sync();

                } catch (IllegalAccessException | NoSuchFieldException | InterruptedException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
        });

        this.ioManager.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP, new EventHandler() {

            @EventHandler.Handle(event = IOEvents.SetupIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP)
            private void handleOutputChannelSetup(final IOEvents.SetupIOEvent event) {

                final OutgoingConnectionType connectionType = event.connectionType;
                final MemoryManager.Allocator allocator = event.allocator;
                final DataWriter.ChannelWriter dataWriter = (DataWriter.ChannelWriter) event.getPayload();

                // Determine the execution unit the given channel is connected
                // to.
                TaskExecutionUnit executionUnit = findTaskExecutionUnitByTaskID(event.srcTaskID);

                EventLoopGroup eventLoopGroup = null;
                if (connectionType instanceof AbstractConnection.LocalConnection) {
                    eventLoopGroup = executionUnit.dataFlowEventLoops.localOutputEventLoopGroup;
                } else {
                    eventLoopGroup = executionUnit.dataFlowEventLoops.networkOutputEventLoopGroup;
                }

                Bootstrap bootstrap = event.connectionType.bootStrap(eventLoopGroup);
                bootstrap.handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline()
                          .addLast(NettyHelper.getLengthDecoder())
                          .addLast(new KryoEventSerializer.KryoOutboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(allocator, null)))
                          .addLast(new KryoEventSerializer.KryoInboundHandler(new KryoEventSerializer.TransferBufferEventSerializer(allocator, null)))
                          .addLast(dataWriter.new OpenCloseGateHandler())
                          .addLast(dataWriter.new WritableHandler());
                    }
                });

                // on success:
                // the close future is registered
                // the polling thread is started
                bootstrap.connect(event.address).addListener(dataWriter.new ConnectListener());
            }
        });
    }
}
