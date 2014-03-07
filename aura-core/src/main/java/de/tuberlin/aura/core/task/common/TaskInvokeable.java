package de.tuberlin.aura.core.task.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.Pool;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;

public abstract class TaskInvokeable {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskInvokeable(final TaskRuntimeContext context, final Logger LOG) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.context = context;

        this.LOG = LOG;

        this.isSuspended = new AtomicBoolean(false);

        this.isRunning = true;

        this.activeGates = new ArrayList<>();

        this.closedGates = new ArrayList<>();

        this.gateCloseFinished = new ArrayList<>();

        this.latch = new CountDownLatch(1);

        for (final List<Descriptors.TaskDescriptor> tdList : context.taskBinding.inputGateBindings) {
            final Map<UUID, Boolean> closedChannels = new HashMap<UUID, Boolean>();
            closedGates.add(closedChannels);

            final Set<UUID> activeChannelSet = new HashSet<UUID>();
            activeGates.add(activeChannelSet);

            gateCloseFinished.add(new AtomicBoolean(false));

            for (final Descriptors.TaskDescriptor td : tdList) {
                closedChannels.put(td.taskID, false);
                activeChannelSet.add(td.taskID);
            }
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TaskRuntimeContext context;

    protected final Logger LOG;

    private final AtomicBoolean isSuspended;

    private final List<Set<UUID>> activeGates;

    private volatile boolean isRunning;

    private final List<AtomicBoolean> gateCloseFinished;

    private final List<Map<UUID, Boolean>> closedGates;

    private final CountDownLatch latch;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public abstract void execute() throws Exception;

    public UUID getTaskID() {
        return context.task.taskID;
    }

    public UUID getOutputTaskID(int gateIndex, int channelIndex) {
        return context.taskBinding.outputGateBindings.get(gateIndex).get(channelIndex).taskID;
    }

    public void emit(int gateIndex, int channelIndex, DataIOEvent event) {
        context.outputGates.get(gateIndex).writeDataToChannel(channelIndex, event);
    }

    public IOEvents.DataBufferEvent absorb(int gateIndex) {
        try {

            if (activeGates.get(gateIndex).size() == 0)
                return null;

            boolean retrieve = true;

            DataIOEvent event = null;

            while (retrieve) {
                event = context.inputGates.get(gateIndex).getInputQueue().take();

                switch (event.type) {
                    case IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED:
                        final Set<UUID> activeChannelSet = activeGates.get(gateIndex);

                        if (!activeChannelSet.remove(event.srcTaskID))
                            throw new IllegalStateException();

                        if (activeChannelSet.isEmpty()) {
                            retrieve = false;
                            event = null;
                        }

                        // check if all gates are exhausted
                        for (final Set<UUID> acs : activeGates) {
                            isRunning &= acs.isEmpty();
                        }
                        isRunning = !isRunning;

                        break;
                    case IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE_FINISHED:
                        final Map<UUID, Boolean> closedChannels = closedGates.get(gateIndex);

                        if (!closedChannels.containsKey(event.srcTaskID))
                            throw new IllegalStateException();

                        closedChannels.put(event.srcTaskID, true);

                        boolean allClosed = true;
                        for (boolean closed : closedChannels.values()) {
                            allClosed &= closed;
                        }

                        if (allClosed) {
                            gateCloseFinished.get(gateIndex).set(true);
                            retrieve = false;
                            event = null;
                        }
                        break;

                    // data event -> absorb
                    default:
                        retrieve = false;
                        break;
                }
            }

            return (IOEvents.DataBufferEvent) event;

        } catch (InterruptedException e) {
            LOG.error(e);
            return null;
        }
    }

    public boolean isTaskRunning() {
        return isRunning;
    }

    public boolean isGateClosed(int gateIndex) {
        return gateCloseFinished.get(gateIndex).get();
    }

    public void openGate(int gateIndex) {
        if (context.inputGates == null) {
            throw new IllegalStateException("Task has no input gates.");
        }
        context.inputGates.get(gateIndex).openGate();
    }

    public Future<?> closeGate(final int gateIndex) {
        if (context.inputGates == null) {
            throw new IllegalStateException("Task has no input gates.");
        }

        return Pool.Manager.executorService.submit(new Runnable() {

            @Override
            public void run() {
                if (gateCloseFinished.get(gateIndex).get()) {
                    LOG.warn("Gate " + gateIndex + " is already closed.");
                } else {
                    gateCloseFinished.get(gateIndex).set(false);
                    context.inputGates.get(gateIndex).closeGate();
                }
                try {
                    latch.await();



                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public int getTaskIndex() {
        return context.task.taskIndex;
    }

    public void suspend() {
        isSuspended.set(true);
    }

    public synchronized void resume() {
        isSuspended.set(false);
        notify();
    }

    public synchronized void cancel() {
        // resume();
        isRunning = false;
    }

    public synchronized void checkIfSuspended() {
        while (isSuspended.get()) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }
}
