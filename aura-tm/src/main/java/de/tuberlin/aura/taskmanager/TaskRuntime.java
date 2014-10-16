package de.tuberlin.aura.taskmanager;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.taskmanager.hdfs.TaskInputSplitProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.drivers.OperatorDriver;
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
import de.tuberlin.aura.drivers.DatasetDriver;


public final class TaskRuntime extends EventDispatcher implements ITaskRuntime {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskRuntime.class);

    private final ITaskManager taskManager;

    private final Descriptors.AbstractNodeDescriptor nodeDescriptor;

    private final Descriptors.NodeBindingDescriptor bindingDescriptor;

    private final QueueManager<IOEvents.DataIOEvent> queueManager;

    private final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM;

    private final IDataProducer producer;

    private final IDataConsumer consumer;

    private Class<? extends AbstractInvokeable> invokeableClazz;

    private AbstractInvokeable invokeable;

    private TaskInputSplitProvider inputSplitProvider;

    private boolean doNextIteration = false;

    private boolean invokeableInitialization = true;

    private CountDownLatch nextIterationSignal = new CountDownLatch(1);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskRuntime(final ITaskManager taskManager,
                       final Descriptors.DeploymentDescriptor deploymentDescriptor) {

        super(true, "TaskDriver-" + deploymentDescriptor.nodeDescriptor.name
                + "-" + deploymentDescriptor.nodeDescriptor.taskIndex
                + "-EventDispatcher");

        // sanity check.
        if (taskManager == null)
            throw new IllegalArgumentException("taskManager == null");

        this.taskManager = taskManager;

        this.nodeDescriptor = deploymentDescriptor.nodeDescriptor;

        this.bindingDescriptor = deploymentDescriptor.nodeBindingDescriptor;

        this.taskFSM = createTaskFSM();

        this.taskFSM.setName("FSM-" + nodeDescriptor.name + "-" + nodeDescriptor.taskIndex + "-EventDispatcher");
       
        this.consumer = new DataConsumer(this);

        this.producer = new DataProducer(this);

        this.queueManager =
                QueueManager.newInstance(nodeDescriptor.taskID,
                        new BlockingSignalQueue.Factory<IOEvents.DataIOEvent>(),
                        new BlockingSignalQueue.Factory<IOEvents.DataIOEvent>());

        inputSplitProvider = new TaskInputSplitProvider(deploymentDescriptor.nodeDescriptor, taskManager.getWorkloadManagerProtocol());

        // ---------------------------------------------------

        addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_EXECUTE_NEXT_ITERATION, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                doNextIteration = (boolean)event.getPayload();
                nextIterationSignal.countDown();
            }
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void initialize(final IAllocator inputAllocator, final IAllocator outputAllocator) {
        // sanity check.
        if (inputAllocator == null)
            throw new IllegalArgumentException("inputAllocator == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        consumer.bind(bindingDescriptor.inputGateBindings, inputAllocator);

        producer.bind(bindingDescriptor.outputGateBindings, outputAllocator);

        // --------------------------------------

        final List<Class<?>> userClasses = new ArrayList<>();

        if (nodeDescriptor.userCodeList != null) {
            final UserCodeImplanter codeImplanter = new UserCodeImplanter(this.getClass().getClassLoader());
            for (final UserCode userCode : nodeDescriptor.userCodeList) {
                // Try to register the bytecode as a class in the JVM.
                final Class<?> userClass = codeImplanter.implantUserCodeClass(userCode);
                if (userClass == null)
                    throw new IllegalArgumentException("userClass == null");
                userClasses.add(userClass);
                if (AbstractInvokeable.class.isAssignableFrom(userClass)) {
                    invokeableClazz = (Class<? extends AbstractInvokeable>) userClass;
                }
            }
            nodeDescriptor.setUserCodeClasses(userClasses);
        }

        // --------------------------------------

        if (nodeDescriptor instanceof Descriptors.InvokeableNodeDescriptor) {

            if (invokeableClazz != null) {
                try {
                    invokeable = invokeableClazz.newInstance();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            } else {
                throw new IllegalStateException();
            }

        } else if (nodeDescriptor instanceof Descriptors.OperatorNodeDescriptor) {

            invokeable = new OperatorDriver(this, (Descriptors.OperatorNodeDescriptor) nodeDescriptor, bindingDescriptor);

            for (final Class<?> udfType : userClasses)
                ((OperatorDriver)invokeable).getExecutionContext().putUDFType(udfType.getName(), udfType);

        } else if (nodeDescriptor instanceof Descriptors.DatasetNodeDescriptor) {

            invokeable = new DatasetDriver(this, (Descriptors.DatasetNodeDescriptor) nodeDescriptor, bindingDescriptor);

        } else {
            throw new IllegalStateException();
        }

        invokeable.setRuntime(this);
        invokeable.setProducer(producer);
        invokeable.setConsumer(consumer);
        invokeable.setLogger(LOG);
    }

    @Override
    public boolean execute() {

        try {

            if (invokeableInitialization) {
                invokeable.create();
                invokeableInitialization = false;
            }

            invokeable.open();

            invokeable.run();

            invokeable.close();

            if (nodeDescriptor.isReExecutable) {

                taskManager.getWorkloadManagerProtocol().doNextIteration(nodeDescriptor.topologyID, nodeDescriptor.taskID);

                try {
                    nextIterationSignal.await();
                } catch (InterruptedException e) {
                    LOG.info(e.getMessage());
                }

                if (!doNextIteration) {
                    invokeable.release();
                } else
                    nextIterationSignal = new CountDownLatch(1);

            } else {

                invokeable.release();
            }

        } catch (final Throwable t) {

            LOG.error(t.getLocalizedMessage(), t);

            taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));

            return false;
        }

        return true;
    }

    @Override
    public void shutdown(boolean awaitExhaustion) {

        if (!(nodeDescriptor instanceof Descriptors.DatasetNodeDescriptor)) {
            invokeable.stopInvokeable();
        }

        producer.shutdown(awaitExhaustion);
        consumer.shutdown();

        consumer.getAllocator().checkForMemoryLeaks();
        producer.getAllocator().checkForMemoryLeaks();
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

    @Override
    public InputSplit getNextInputSplit() {
        return inputSplitProvider.getNextInputSplit();
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
    public IDataProducer getProducer() {
        return producer;
    }

    @Override
    public IDataConsumer getConsumer() {
        return consumer;
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

    @Override
    public boolean doNextIteration() {
        return doNextIteration;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

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
                        .and().addTransition(TaskTransition.TASK_TRANSITION_NEXT_ITERATION, TaskState.TASK_STATE_READY)
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

        taskFSM.addStateListener(TaskState.ERROR, new StateMachine.IFSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                throw new IllegalStateException("Task " + nodeDescriptor.name + " [" + nodeDescriptor.taskID + "] from state " + previousState
                        + " to " + state + " is not defined  [" + transition.toString() + "]");
            }
        });

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
