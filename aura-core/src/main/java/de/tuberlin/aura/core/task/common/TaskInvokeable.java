package de.tuberlin.aura.core.task.common;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;

public abstract class TaskInvokeable {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public TaskInvokeable(final TaskContext context, final Logger LOG) {
		// sanity check.
		if (context == null)
			throw new IllegalArgumentException("context == null");
		if (LOG == null)
			throw new IllegalArgumentException("LOG == null");

		this.context = context;

		this.LOG = LOG;

		this.isSuspended = new AtomicBoolean(false);
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	protected final TaskContext context;

	protected final Logger LOG;

	private AtomicBoolean isSuspended;

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

	public void emit(int gateIndex, int channelIndex, DataIOEvent buffer) {
		context.outputGates.get(gateIndex).writeDataToChannel(channelIndex, buffer);
	}

	public DataIOEvent absorb(int gateIndex) {
		try {
			final DataIOEvent event = context.inputGates.get(gateIndex).getInputQueue().take();
			return event;
		} catch (InterruptedException e) {
			LOG.error(e);
			return null;
		}
	}

	public void openGate(int channelIndex) {
		context.inputGates.get(channelIndex).openGate();
	}

	public void closeGate(int channelIndex) {
		context.inputGates.get(channelIndex).closeGate();
	}

	public void suspend() {
		isSuspended.set(true);
	}

	public synchronized void resume() {
	      isSuspended.set(false);
	      notify();
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
