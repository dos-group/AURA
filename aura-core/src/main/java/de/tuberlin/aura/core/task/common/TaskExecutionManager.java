package de.tuberlin.aura.core.task.common;


import java.lang.reflect.Field;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitLocalInputEventLoopGroup;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitNetworkInputEventLoopGroup;
import de.tuberlin.aura.core.memory.MemoryManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
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

        LOG.info("EXECUTE TASK " + driverContext.taskDescriptor.name + " [" + driverContext.taskDescriptor.taskID + "]" + " ON EXECUTION UNIT ("
                + executionUnit[selectedEU].getExecutionUnitID() + ") ON MACHINE [" + machineDescriptor.uid + "]");
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

        LOG.warn("No task execution unit was found for this task ID: {}", taskID);
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

            @EventHandler.Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_SETUP)
            private void handleInputChannelSetup(final IOEvents.DataIOEvent event) {

                try {
                    // Add the channel to the according event loop group.
                    Channel channel = event.getChannel();

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
                        public void operationComplete(ChannelFuture future) throws Exception {
                            try {
                                Channel channel = future.channel();
                                LOG.trace("Change event loop from {} to event loop of task {}", channel.eventLoop().parent(), event.dstTaskID);

                                // Determine the execution unit the given channel is connected to.
                                TaskExecutionUnit executionUnit = findTaskExecutionUnitByTaskID(event.dstTaskID);

                                if (channel instanceof LocalChannel) {
                                    ExecutionUnitLocalInputEventLoopGroup eventLoopGroup = executionUnit.dataFlowEventLoops.localInputEventLoopGroup;

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

                                // Enable auto read again.
                                channel.config().setAutoRead(true);

                                LOG.trace("Changed event loop to {}", channel.eventLoop().parent());
                            } catch (Throwable t) {
                                LOG.error(t.getLocalizedMessage(), t);
                                throw t;
                            }
                        }
                    })
                           .sync();
                } catch (InterruptedException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } catch (Throwable e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }

                // Dispatch INPUT_CHANNEL_CONNECTED event.
                IOEvents.GenericIOEvent connected =
                        new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
                                                    event.getPayload(),
                                                    event.srcTaskID,
                                                    event.dstTaskID);
                connected.setChannel(event.getChannel());
                ioManager.dispatchEvent(connected);
            }
        });

        this.ioManager.addEventListener(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP, new EventHandler() {

            @EventHandler.Handle(event = IOEvents.DataIOEvent.class, type = IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_SETUP)
            private void handleOutputChannelSetup(final IOEvents.DataIOEvent event) {

                try {
                    // Add the channel to the according event loop group.
                    Channel channel = event.getChannel();

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
                        public void operationComplete(ChannelFuture future) throws Exception {
                            try {
                                Channel channel = future.channel();
                                LOG.debug("Change event loop from {} to event loop of task {}", channel.eventLoop().parent(), event.srcTaskID);

                                // Determine the execution unit the given channel is connected to.
                                TaskExecutionUnit executionUnit = findTaskExecutionUnitByTaskID(event.srcTaskID);

                                EventLoopGroup eventLoopGroup;
                                if (channel instanceof LocalChannel) {
                                    eventLoopGroup = executionUnit.dataFlowEventLoops.localOutputEventLoopGroup;

                                    // TODO: Dirty dirty hack... [will be fixed in Netty
                                    // 4.0.19.Final]
                                    Class<?> clazz = channel.getClass();
                                    Field stateField = clazz.getDeclaredField("state");
                                    stateField.setAccessible(true);
                                    stateField.setInt(channel, 0);

                                    Field peerField = clazz.getDeclaredField("peer");
                                    peerField.setAccessible(true);
                                    final LocalChannel peer = (LocalChannel) peerField.get(channel);

                                    peerField.set(channel, null);

                                    // Change event loop group.
                                    eventLoopGroup.register(channel).sync();

                                    // Activate the channels
                                    stateField.setInt(channel, 2);
                                    stateField.setInt(peer, 2);

                                    peerField.set(channel, peer);
                                    peer.eventLoop().execute(new Runnable() {

                                        @Override
                                        public void run() {
                                            peer.pipeline().fireChannelActive();
                                        }
                                    });
                                } else {
                                    eventLoopGroup = executionUnit.dataFlowEventLoops.networkOutputEventLoopGroup;

                                    // Change event loop group.
                                    eventLoopGroup.register(channel).sync();
                                }

                                // Enable auto read again.
                                channel.config().setAutoRead(true);

                                LOG.debug("Changed event loop to {}", channel.eventLoop().parent());
                            } catch (Throwable t) {
                                LOG.error(t.getLocalizedMessage(), t);
                                throw t;
                            }
                        }
                    }).sync();
                } catch (InterruptedException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                } catch (Throwable e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }

                // Dispatch OUTPUT_CHANNEL_CONNECTED event.
                final IOEvents.GenericIOEvent connected =
                        new IOEvents.GenericIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
                                                    event.getPayload(),
                                                    event.srcTaskID,
                                                    event.dstTaskID);
                connected.setChannel(event.getChannel());
                ioManager.dispatchEvent(connected);
            }
        });
    }
}