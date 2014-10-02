package de.tuberlin.aura.taskmanager;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.operators.OperatorDriver;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.iosystem.queues.BlockingSignalQueue;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.taskmanager.common.TaskStates.TaskState;
import de.tuberlin.aura.core.taskmanager.common.TaskStates.TaskTransition;
import de.tuberlin.aura.core.taskmanager.spi.*;
import de.tuberlin.aura.core.taskmanager.usercode.UserCode;
import de.tuberlin.aura.core.taskmanager.usercode.UserCodeImplanter;
import de.tuberlin.aura.datasets.DatasetDriver;

/**
 *
 */
public final class TaskDriver extends EventDispatcher implements ITaskDriver {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

    private final ITaskManager taskManager;

    private final Descriptors.AbstractNodeDescriptor nodeDescriptor;

    private final Descriptors.NodeBindingDescriptor bindingDescriptor;

    private final QueueManager<IOEvents.DataIOEvent> queueManager;

    private final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM;

    private final IDataProducer dataProducer;

    private final IDataConsumer dataConsumer;

    private Class<? extends AbstractInvokeable> invokeableClazz;

    private AbstractInvokeable invokeable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDriver(final ITaskManager taskManager, final Descriptors.DeploymentDescriptor deploymentDescriptor) {
        super(true, "TaskDriver-" + deploymentDescriptor.nodeDescriptor.name + "-" + deploymentDescriptor.nodeDescriptor.taskIndex
                + "-EventDispatcher");

        // sanity check.
        if (taskManager == null)
            throw new IllegalArgumentException("taskManager == null");

        this.taskManager = taskManager;

        this.nodeDescriptor = deploymentDescriptor.nodeDescriptor;

        this.bindingDescriptor = deploymentDescriptor.nodeBindingDescriptor;

        this.taskFSM = createTaskFSM();

        this.taskFSM.setName("FSM-" + nodeDescriptor.name + "-" + nodeDescriptor.taskIndex + "-EventDispatcher");
       
        dataConsumer = new TaskDataConsumer(this);

        dataProducer = new TaskDataProducer(this);

        this.queueManager =
                QueueManager.newInstance(nodeDescriptor.taskID,
                        new BlockingSignalQueue.Factory<IOEvents.DataIOEvent>(),
                        new BlockingSignalQueue.Factory<IOEvents.DataIOEvent>());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    // ------------- Task Driver Lifecycle ---------------

    @Override
    @SuppressWarnings("unchecked")
    public void startupDriver(final IAllocator inputAllocator, final IAllocator outputAllocator) {
        // sanity check.
        if (inputAllocator == null)
            throw new IllegalArgumentException("inputAllocator == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        dataConsumer.bind(bindingDescriptor.inputGateBindings, inputAllocator);

        dataProducer.bind(bindingDescriptor.outputGateBindings, outputAllocator);

        // --------------------------------------

        final List<Class<?>> userClasses = new ArrayList<>();

        if (nodeDescriptor.userCodeList != null) {
            for (final UserCode uc : nodeDescriptor.userCodeList) {
                final Class<?> userClass = implantInvokeableCode(uc);
                userClasses.add(userClass);
                if (AbstractInvokeable.class.isAssignableFrom(userClass)) {
                    invokeableClazz = (Class<? extends AbstractInvokeable>) userClass;
                }
            }
        }

        nodeDescriptor.setUserCodeClasses(userClasses);

        // --------------------------------------

        if (nodeDescriptor instanceof Descriptors.InvokeableNodeDescriptor) {

            if (invokeableClazz != null) {
                invokeable = createInvokeable(invokeableClazz, this, dataProducer, dataConsumer, LOG);
                if (invokeable == null)
                    throw new IllegalStateException("invokeable == null");
            } else {
                throw new IllegalStateException();
            }

        } else if (nodeDescriptor instanceof Descriptors.OperatorNodeDescriptor) {

            if (invokeableClazz != null)
                throw new IllegalStateException();

            invokeable = new OperatorDriver((Descriptors.OperatorNodeDescriptor) nodeDescriptor);
            invokeable.setTaskDriver(this);
            invokeable.setDataProducer(dataProducer);
            invokeable.setDataConsumer(dataConsumer);
            invokeable.setLogger(LOG);

            for (final Class<?> udfType : userClasses)
                ((OperatorDriver)invokeable).getOperatorEnvironment().putUdfType(udfType.getName(), udfType);

        } else if (nodeDescriptor instanceof Descriptors.DatasetNodeDescriptor) {

            invokeable = new DatasetDriver((Descriptors.DatasetNodeDescriptor) nodeDescriptor, bindingDescriptor);
            invokeable.setTaskDriver(this);
            invokeable.setDataProducer(dataProducer);
            invokeable.setDataConsumer(dataConsumer);
            invokeable.setLogger(LOG);

        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean executeDriver() {

        try {

            invokeable.create();

            invokeable.open();

            invokeable.run();

            invokeable.close();

            invokeable.release();

        } catch (final Throwable t) {

            LOG.error(t.getLocalizedMessage(), t);

            taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));

            return false;
        }

        return true;
    }

    @Override
    public void teardownDriver(boolean awaitExhaustion) {

        if (!(nodeDescriptor instanceof Descriptors.DatasetNodeDescriptor)) {
            invokeable.stopInvokeable();
        }

        dataProducer.shutdownProducer(awaitExhaustion);
        dataConsumer.shutdownConsumer();

        // Check for memory leaks in the input/output allocator.
        dataConsumer.getAllocator().checkForMemoryLeaks();
        dataProducer.getAllocator().checkForMemoryLeaks();

        if (nodeDescriptor instanceof Descriptors.InvokeableNodeDescriptor
            || nodeDescriptor instanceof Descriptors.OperatorNodeDescriptor) {
            // TODO: Wait until all gates are closed? -> invokeable.close() emits all DATA_EVENT_SOURCE_EXHAUSTED events
            taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));
        }
    }

    // ---------------------------------------------------

    @Override
    public void connectDataChannel(final Descriptors.AbstractNodeDescriptor dstNodeDescriptor, final IAllocator allocator) {
        // sanity check.
        if(dstNodeDescriptor == null)
            throw new IllegalArgumentException("dstNodeDescriptor == null");
        if(allocator == null)
            throw new IllegalArgumentException("allocator == null");

        taskManager.getIOManager().connectDataChannel(
                nodeDescriptor.taskID,
                dstNodeDescriptor.taskID,
                dstNodeDescriptor.getMachineDescriptor()
        );
    }

    // ---------------------------------------------------
    // Public Getter Methods.
    // ---------------------------------------------------

    @Override
    public Descriptors.AbstractNodeDescriptor getNodeDescriptor() {
        return nodeDescriptor;
    }

    @Override
    public Descriptors.NodeBindingDescriptor getBindingDescriptor() {
        return bindingDescriptor;
    }

    @Override
    public QueueManager<IOEvents.DataIOEvent> getQueueManager() {
        return queueManager;
    }

    @Override
    public StateMachine.FiniteStateMachine getTaskStateMachine() {
        return taskFSM;
    }

    @Override
    public IDataProducer getDataProducer() {
        return dataProducer;
    }

    @Override
    public IDataConsumer getDataConsumer() {
        return dataConsumer;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public ITaskManager getTaskManager() {
        return taskManager;
    }

    @Override
    public AbstractInvokeable getInvokeable() {
        return invokeable;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Class<?> implantInvokeableCode(final UserCode userCode) {
        // Try to register the bytecode as a class in the JVM.
        final UserCodeImplanter codeImplanter = new UserCodeImplanter(this.getClass().getClassLoader());
        @SuppressWarnings("unchecked")
        final Class<?> userCodeClazz = codeImplanter.implantUserCodeClass(userCode);
        // sanity check.
        if (userCodeClazz == null)
            throw new IllegalArgumentException("userCodeClazz == null");
        return userCodeClazz;
    }

    private AbstractInvokeable createInvokeable(final Class<? extends AbstractInvokeable> invokableClazz,
                                                final ITaskDriver taskDriver,
                                                final IDataProducer dataProducer,
                                                final IDataConsumer dataConsumer,
                                                final Logger LOG) {
        try {
            final AbstractInvokeable invokeable = invokableClazz.newInstance();
            invokeable.setTaskDriver(taskDriver);
            invokeable.setDataProducer(dataProducer);
            invokeable.setDataConsumer(dataConsumer);
            invokeable.setLogger(LOG);
            return invokeable;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private StateMachine.FiniteStateMachine<TaskState, TaskTransition> createTaskFSM() {

        final StateMachine.FiniteStateMachineBuilder<TaskState, TaskTransition> taskFSMBuilder =
                new StateMachine.FiniteStateMachineBuilder<>(TaskState.class, TaskTransition.class, TaskState.ERROR);

        final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM =
                taskFSMBuilder.defineState(TaskState.TASK_STATE_CREATED)
                        .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_INPUTS_CONNECTED)
                        .and().addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_OUTPUTS_CONNECTED)
                        .defineState(TaskState.TASK_STATE_INPUTS_CONNECTED)
                        .addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                        .defineState(TaskState.TASK_STATE_OUTPUTS_CONNECTED)
                        .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                        .defineState(TaskState.TASK_STATE_READY)
                        .addTransition(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_RUNNING)
                        .defineState(TaskState.TASK_STATE_RUNNING)
                        .addTransition(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_FINISHED)
                        .and().addTransition(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED)
                        .and().addTransition(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE)
                        .and().addTransition(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_PAUSED)
                        .defineState(TaskState.TASK_STATE_FINISHED)
                        .noTransition()
                        .defineState(TaskState.TASK_STATE_CANCELED)
                        .noTransition()
                        .defineState(TaskState.TASK_STATE_FAILURE)
                        .noTransition()
                        .defineState(TaskState.TASK_STATE_PAUSED)
                        .addTransition(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_RUNNING)
                        .setInitialState(TaskState.TASK_STATE_CREATED)
                        .build();

        // global state listener, that reacts to all state changes.

        taskFSM.addGlobalStateListener(new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                try {
                    LOG.info("CHANGE STATE OF TASK " + nodeDescriptor.name + " [" + nodeDescriptor.taskID + "] FROM " + previousState + " TO "
                            + state + "  [" + transition.toString() + "]");
                    final IOEvents.TaskControlIOEvent stateUpdate =
                            new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE);
                    stateUpdate.setPayload(state);
                    stateUpdate.setTaskID(nodeDescriptor.taskID);
                    stateUpdate.setTopologyID(nodeDescriptor.topologyID);
                    taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), stateUpdate);
                } catch (Throwable t) {
                    LOG.error(t.getLocalizedMessage(), t);
                    throw t;
                }
            }
        });

        // error state listener.

        taskFSM.addStateListener(TaskState.ERROR, new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                throw new IllegalStateException("Task " + nodeDescriptor.name + " [" + nodeDescriptor.taskID + "] from state " + previousState
                        + " to " + state + " is not defined  [" + transition.toString() + "]");
            }
        });

        // taskmanager ready state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_READY, new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);
                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
                transitionUpdate.setTaskID(nodeDescriptor.taskID);
                transitionUpdate.setTopologyID(nodeDescriptor.topologyID);
                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        // taskmanager finish state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_FINISHED, new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);
                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));
                transitionUpdate.setTaskID(nodeDescriptor.taskID);
                transitionUpdate.setTopologyID(nodeDescriptor.topologyID);
                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        // taskmanager failure state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_FAILURE, new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);
                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));
                transitionUpdate.setTaskID(nodeDescriptor.taskID);
                transitionUpdate.setTopologyID(nodeDescriptor.topologyID);
                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        return taskFSM;
    }
}
