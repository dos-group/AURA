package de.tuberlin.aura.taskmanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskStateMachine;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import de.tuberlin.aura.taskmanager.TaskEvents.TaskStateTransitionEvent;

public final class TaskManager implements IEventHandler, WM2TMProtocol {

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	/**
     *
     */
	private final class IORedispatcher extends EventHandler {

		@Handle(event = IOEvents.IODataChannelEvent.class)
		private void handleDataChannelEvent(
				final IOEvents.IODataChannelEvent event) {

			Pair<TaskContext, IEventDispatcher> contextAndHandler = null;
			// Call the correct handler!
			if (IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_CHANNEL_CONNECTED
				.equals(event.type))
				contextAndHandler = taskContextMap.get(event.srcTaskID);
			if (IOEvents.IODataChannelEvent.IO_EVENT_INPUT_CHANNEL_CONNECTED
				.equals(event.type))
				contextAndHandler = taskContextMap.get(event.dstTaskID);
			// check state.
			if (contextAndHandler == null)
				throw new IllegalStateException("contextAndHandler for task "
					+ event.dstTaskID + " == null");
			final IEventDispatcher dispatcher = contextAndHandler.getSecond();
			dispatcher.dispatchEvent(event);
		}

		@Handle(event = IOEvents.IODataEvent.class)
		private void handleDataEvent(final IOEvents.IODataEvent event) {

			final Pair<TaskContext, IEventDispatcher> contextAndHandler = taskContextMap
				.get(event.message.dstTaskID);
			contextAndHandler.getSecond().dispatchEvent(event);
		}
	}

	/**
     *
     */
	private final class TaskEventHandler extends EventHandler {

		public TaskContext context;

		@Handle(event = IOEvents.IODataChannelEvent.class, type = IOEvents.IODataChannelEvent.IO_EVENT_INPUT_CHANNEL_CONNECTED)
		private void handleTaskInputDataChannelConnect(
				final IOEvents.IODataChannelEvent event) {
			int gateIndex = 0;
			boolean allInputGatesConnected = true, connectingToCorrectTask = false;
			for (final List<TaskDescriptor> inputGate : context.taskBinding.inputGateBindings) {
				int channelIndex = 0;
				boolean allInputChannelsPerGateConnected = true;
				for (TaskDescriptor inputTask : inputGate) {
					// Set the channel on right position.
					if (inputTask.uid.equals(event.srcTaskID)) {
						context.inputGates.get(gateIndex).setChannel(
							channelIndex, event.channel);
						LOG.info("input connection from " + inputTask.name
							+ " [" + inputTask.uid + "] to task "
							+ context.task.name + " [" + context.task.uid
							+ "] is established");
						connectingToCorrectTask |= true;
					}
					// all data outputs are connected...
					allInputChannelsPerGateConnected &= (context.inputGates
						.get(gateIndex).getChannel(channelIndex++) != null);
				}

				allInputGatesConnected &= allInputChannelsPerGateConnected;
				++gateIndex;
			}

			// Check if the incoming channel is connecting to the correct task.
			if (!connectingToCorrectTask)
				throw new IllegalStateException(
					"wrong data channel tries to connect");

			if (allInputGatesConnected) {
				context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(
					TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
			}
		}

		@Handle(event = IOEvents.IODataChannelEvent.class, type = IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_CHANNEL_CONNECTED)
		private void handleTaskOutputDataChannelConnect(
				final IOEvents.IODataChannelEvent event) {
			int gateIndex = 0;
			boolean allOutputGatesConnected = true;
			for (final List<TaskDescriptor> outputGate : context.taskBinding.outputGateBindings) {
				int channelIndex = 0;
				boolean allOutputChannelsPerGateConnected = true;
				for (TaskDescriptor outputTask : outputGate) {
					// Set the channel on right position.
					if (outputTask.uid.equals(event.dstTaskID)) {
						context.outputGates.get(gateIndex).setChannel(
							channelIndex, event.channel);
						LOG.info("output connection from " + context.task.name
							+ " [" + context.task.uid + "] to task "
							+ outputTask.name + " [" + outputTask.uid
							+ "] is established");
					}
					// all data outputs are connected...
					allOutputChannelsPerGateConnected &= (context.outputGates
						.get(gateIndex).getChannel(channelIndex++) != null);
				}
				allOutputGatesConnected &= allOutputChannelsPerGateConnected;
				++gateIndex;
			}

			if (allOutputGatesConnected) {
				context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(
					TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED));
			}
		}

		@Handle(event = IOEvents.IODataEvent.class)
		private void handleTaskInputData(final IOEvents.IODataEvent event) {
			context.inputGates
				.get(context
					.getInputChannelIndexFromTaskID(event.message.srcTaskID))
				.addToInputQueue(event.message);
		}

		@Handle(event = TaskStateTransitionEvent.class)
		private void handleTaskStateTransition(
				final TaskStateTransitionEvent event) {
			synchronized (context.state) { // serialize task state transitions!
				final TaskState oldState = context.state;
				final Map<TaskTransition, TaskState> transitionsSpace = TaskStateMachine.TASK_STATE_TRANSITION_MATRIX
					.get(context.state);
				final TaskState nextState = transitionsSpace
					.get(event.transition);
				context.state = nextState;
				// Trigger state dependent actions. Realization of a classic
				// Moore automata.
				switch (context.state) {
				case TASK_STATE_NOT_CONNECTED: {
				}
					break;
				case TASK_STATE_INPUTS_CONNECTED: {
				}
					break;
				case TASK_STATE_OUTPUTS_CONNECTED: {
				}
					break;
				case TASK_STATE_READY: {
					scheduleTask(context);
				}
					break;
				case TASK_STATE_RUNNING: {
				}
					break;
				case TASK_STATE_FINISHED: {
					// for( final Channel ch : context.outputChannel )
					// ch.close();
				}
					break;
				case TASK_STATE_FAILURE: {
				}
					break;
				case TASK_STATE_RECOVER: {
				}
					break;
				case TASK_STATE_UNDEFINED: {
					throw new IllegalStateException("task " + context.task.name
						+ " [" + context.task.uid + "] from state "
						+ oldState + " to " + context.state
						+ " is not defined");
				}
				}
				LOG.info("change state of task " + context.task.name + " ["
					+ context.task.uid + "] from " + oldState + " to "
					+ context.state);
			}
		}
	}

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public TaskManager(String zkServers, final MachineDescriptor machine) {
		// sanity check.
		if (machine == null)
			throw new IllegalArgumentException("machine == null");
		ZkHelper.checkConnectionString(zkServers);

		this.taskContextMap = new ConcurrentHashMap<UUID, Pair<TaskContext, IEventDispatcher>>();

		this.ioManager = new IOManager(machine);

		this.rpcManager = new RPCManager(ioManager);

		this.ioHandler = new IORedispatcher();

		this.codeImplanter = new UserCodeImplanter(this.getClass()
			.getClassLoader());

		final int N = 4;
		this.executionUnit = new TaskExecutionUnit[N];
		for (int i = 0; i < N; ++i) {
			this.executionUnit[i] = new TaskExecutionUnit(i);
			this.executionUnit[i].start();
		}

		registerIOEvents(ioHandler);

		rpcManager.registerRPCProtocolImpl(this, WM2TMProtocol.class);

		// TaskEventHandler teh = new TaskEventHandler();

		// TODO: move this into a seperate mehtod.
		try {
			// Get a connection to ZooKeeper and initialize the directories in
			// ZooKeeper.
			this.zk = new ZooKeeper(zkServers, ZkHelper.ZOOKEEPER_TIMEOUT,
				new ZkConnectionWatcher(this));
			ZkHelper.initDirectories(this.zk);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(machine);
			oos.flush();

			this.zk.create(ZkHelper.ZOOKEEPER_TASKMANAGERS + "/"
				+ machine.uid.toString(), baos.toByteArray(),
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

			oos.close();
			baos.close();
		} catch (IOException e) {
			LOG.error("Couldn't connect to ZooKeeper", e);
		} catch (KeeperException e) {
			LOG.error("An error occurred in ZooKeeper", e);
		} catch (InterruptedException e) {
			LOG.error("The connection to ZooKeeper was interrupted.", e);
		}
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(TaskManager.class);

	private final Map<UUID, Pair<TaskContext, IEventDispatcher>> taskContextMap;

	private final IOManager ioManager;

	private final IORedispatcher ioHandler;

	private final RPCManager rpcManager;

	private final TaskExecutionUnit[] executionUnit;

	private final UserCodeImplanter codeImplanter;

	/**
	 * The connection to the ZooKeeper-cluster.
	 */
	private ZooKeeper zk;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	@Override
	public void installTask(
			final TaskDeploymentDescriptor taskDeploymentDescriptor) {
		// sanity check.
		if (taskDeploymentDescriptor == null)
			throw new IllegalArgumentException("taskDescriptor == null");

		@SuppressWarnings("unchecked")
		final Class<? extends TaskInvokeable> userCodeClass = (Class<? extends TaskInvokeable>) codeImplanter
			.implantUserCodeClass(taskDeploymentDescriptor.taskDescriptor.userCode);

		installTask(taskDeploymentDescriptor.taskDescriptor,
			taskDeploymentDescriptor.taskBindingDescriptor, userCodeClass);
	}

	// TODO: Make that later private!
	public void installTask(final TaskDescriptor taskDescriptor,
			final TaskBindingDescriptor taskBindingDescriptor,
			final Class<? extends TaskInvokeable> executableClass) {

		final TaskEventHandler handler = new TaskEventHandler();
		final IEventDispatcher dispatcher = registerTaskEvents(
			new EventDispatcher(true), handler);
		final TaskContext taskContext = new TaskContext(taskDescriptor,
			taskBindingDescriptor, handler, dispatcher, executableClass);
		handler.context = taskContext;
		taskContextMap
			.put(taskDescriptor.uid,
				new Pair<TaskContext, IEventDispatcher>(taskContext,
					dispatcher));

		if (taskBindingDescriptor.inputGateBindings.size() == 0) {
			taskContext.dispatcher.dispatchEvent(new TaskStateTransitionEvent(
				TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
		}

		if (taskBindingDescriptor.outputGateBindings.size() == 0) {
			taskContext.dispatcher.dispatchEvent(new TaskStateTransitionEvent(
				TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED));
		}

		// TODO: To allow cycles in the execution graph we have to split up
		// installation and wiring of tasks in the deployment phase!
		wireOutputDataChannels(taskDescriptor, taskBindingDescriptor);
	}

	public RPCManager getRPCManager() {
		return rpcManager;
	}

	@Override
	public void handleEvent(Event event) {
		switch (event.type) {
		case ZkHelper.EVENT_TYPE_CONNECTION_EXPIRED:
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		default:
			// Nothing to do here.
		}
	}

	// ---------------------------------------------------
	// Private.
	// ---------------------------------------------------

	private synchronized void wireOutputDataChannels(
			final TaskDescriptor taskDescriptor,
			final TaskBindingDescriptor taskBindingDescriptor) {

		// Connect outputs, if we have some...
		if (taskBindingDescriptor.outputGateBindings.size() > 0) {
			for (final List<TaskDescriptor> outputGate : taskBindingDescriptor.outputGateBindings)
				for (final TaskDescriptor outputTask : outputGate)
					ioManager.connectDataChannel(taskDescriptor.uid,
						outputTask.uid, outputTask.getMachineDescriptor());
		}
	}

	private void registerIOEvents(final IEventHandler handler) {
		this.ioManager.addEventListener(
			IOEvents.IODataChannelEvent.IO_EVENT_INPUT_CHANNEL_CONNECTED,
			handler);
		this.ioManager.addEventListener(
			IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_CHANNEL_CONNECTED,
			handler);
		this.ioManager.addEventListener(
			IOEvents.IODataEvent.IO_EVENT_RECEIVED_DATA, handler);
	}

	private IEventDispatcher registerTaskEvents(
			final IEventDispatcher dispatcher, final IEventHandler handler) {
		dispatcher.addEventListener(
			IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_CHANNEL_CONNECTED,
			handler);
		dispatcher.addEventListener(
			IOEvents.IODataChannelEvent.IO_EVENT_INPUT_CHANNEL_CONNECTED,
			handler);
		dispatcher.addEventListener(
			IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_GATE_OPEN, handler);
		dispatcher
			.addEventListener(
				IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_GATE_CLOSE,
				handler);
		dispatcher.addEventListener(
			IOEvents.IODataEvent.IO_EVENT_RECEIVED_DATA, handler);
		dispatcher.addEventListener(
			TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT, handler);
		return dispatcher;
	}

	private void scheduleTask(final TaskContext context) {
		// sanity check.
		if (context == null)
			throw new IllegalArgumentException("context must not be null");
		final int N = 4;
		int tmpMin, tmpMinOld;
		tmpMin = tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
		int selectedEU = 0;
		for (int i = 1; i < N; ++i) {
			tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
			if (tmpMin < tmpMinOld) {
				tmpMinOld = tmpMin;
				selectedEU = i;
			}
		}
		executionUnit[selectedEU].enqueueTask(context);
		LOG.info("execute task " + context.task.name + " [" + context.task.uid
			+ "]" + " on ExecutionUnit ("
			+ executionUnit[selectedEU].getExecutionUnitID() + ")");
	}
}
