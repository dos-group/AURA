package de.tuberlin.aura.core.task.common;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import org.apache.log4j.Logger;

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

        this.activeGates = new ArrayList<Set<UUID>>();

        for(final List<Descriptors.TaskDescriptor> tdList : context.taskBinding.inputGateBindings) {
            final Set<UUID> activeChannelSet = new HashSet<UUID>();
            activeGates.add(activeChannelSet);
            for(final Descriptors.TaskDescriptor td : tdList) {
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

	public DataIOEvent absorb(int gateIndex) {
		try {

            if (activeGates.get(gateIndex).size() == 0)
                return null;

            final DataIOEvent event = context.inputGates.get(gateIndex).getInputQueue().take();

            // TODO: Is that the right place?
            if (IOEvents.DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(event.type)) {

                final Set<UUID> activeChannelSet = activeGates.get(gateIndex);

                if (!activeChannelSet.remove(event.srcTaskID))
                    throw new IllegalStateException();

                for(final Set<UUID> acs : activeGates) {
                    isRunning &= acs.isEmpty();
                }
                isRunning = !isRunning;
            }

            return event;
		} catch (InterruptedException e) {
			LOG.error(e);
			return null;
		}
	}

    public boolean isTaskRunning() {
        return isRunning;
    }

	public void openGate(int channelIndex) {
		context.inputGates.get(channelIndex).openGate();
	}

	public void closeGate(int channelIndex) {
		context.inputGates.get(channelIndex).closeGate();
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
        //resume();
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
