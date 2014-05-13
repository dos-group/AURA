package de.tuberlin.aura.taskmanager;


import java.lang.reflect.Constructor;

import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.task.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BlockingBufferQueue;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.measurement.MeasurementManager;
import de.tuberlin.aura.core.measurement.record.RecordReader;
import de.tuberlin.aura.core.measurement.record.RecordWriter;
import de.tuberlin.aura.core.task.common.TaskStates.TaskState;
import de.tuberlin.aura.core.task.common.TaskStates.TaskTransition;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;

/**
 *
 */
public final class TaskDriver extends EventDispatcher implements ITaskDriver {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

    private final ITaskManager taskManager;

    private final Descriptors.TaskDescriptor taskDescriptor;

    private final Descriptors.TaskBindingDescriptor taskBindingDescriptor;

    private final QueueManager<IOEvents.DataIOEvent> queueManager;

    private final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM;


    private IDataProducer dataProducer;

    private IDataConsumer dataConsumer;

    private Class<? extends AbstractTaskInvokeable> invokeableClazz;

    private AbstractTaskInvokeable invokeable;


    // Measurement stuff...

    private final MeasurementManager measurementManager;

    private final RecordReader recordReader;

    private final RecordWriter recordWriter;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDriver(final ITaskManager taskManager, final Descriptors.TaskDeploymentDescriptor deploymentDescriptor) {
        super(true, "TaskDriver-" + deploymentDescriptor.taskDescriptor.name + "-" + deploymentDescriptor.taskDescriptor.taskIndex
                + "-EventDispatcher");

        // sanity check.
        if (taskManager == null)
            throw new IllegalArgumentException("taskManager == null");
        if (deploymentDescriptor == null)
            throw new IllegalArgumentException("deploymentDescriptor == null");

        this.taskManager = taskManager;

        this.taskDescriptor = deploymentDescriptor.taskDescriptor;

        this.taskBindingDescriptor = deploymentDescriptor.taskBindingDescriptor;

        this.taskFSM = createTaskFSM();

        this.taskFSM.setName("FSM-" + taskDescriptor.name + "-" + taskDescriptor.taskIndex + "-EventDispatcher");

        this.measurementManager = MeasurementManager.getInstance("/tm/" + taskDescriptor.name + "_" + taskDescriptor.taskIndex, "Task");
        MeasurementManager.registerListener(MeasurementManager.TASK_FINISHED + "-" + taskDescriptor.taskID + "-" + taskDescriptor.name + "-"
                + taskDescriptor.taskIndex, measurementManager);

        this.recordReader = new RecordReader();

        this.recordWriter = new RecordWriter();

        this.queueManager =
                QueueManager.newInstance(taskDescriptor.taskID, new BlockingBufferQueue.Factory<IOEvents.DataIOEvent>(), measurementManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    // ------------- Task Driver Lifecycle ---------------

    /**
     *
     */
    @Override
    public void startupDriver(final IAllocator inputAllocator, final IAllocator outputAllocator) {
        // sanity check.
        if (inputAllocator == null)
            throw new IllegalArgumentException("inputAllocator == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        dataConsumer = new TaskDataConsumer(this, inputAllocator);

        dataProducer = new TaskDataProducer(this, outputAllocator);

        // TODO: A Task can in future contain a list of associated user code.
        invokeableClazz = implantInvokeableCode(taskDescriptor.userCodeList.get(0));

        invokeable = createInvokeable(invokeableClazz, this, dataProducer, dataConsumer, LOG);

        if (invokeable == null)
            throw new IllegalStateException("invokeable == null");
    }

    /**
     *
     */
    @Override
    public void executeDriver() {

        try {

            invokeable.create();

            invokeable.open();

            invokeable.run();

            invokeable.close();

            invokeable.release();

        } catch (final Throwable t) {

            LOG.error(t.getLocalizedMessage(), t);

            taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));

            return;
        }

        // TODO: Wait until all gates are closed? -> invokeable.close() emits all
        // DATA_EVENT_SOURCE_EXHAUSTED events
        taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));
    }

    /**
     * @param awaitExhaustion
     */
    @Override
    public void teardownDriver(boolean awaitExhaustion) {

        invokeable.stopInvokeable();

        dataProducer.shutdownProducer(awaitExhaustion);

        dataConsumer.shutdownConsumer();
    }

    // ---------------------------------------------------

    @Override
    public Descriptors.TaskDescriptor getTaskDescriptor() {
        return taskDescriptor;
    }

    @Override
    public Descriptors.TaskBindingDescriptor getTaskBindingDescriptor() {
        return taskBindingDescriptor;
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
    public void connectDataChannel(final Descriptors.TaskDescriptor dstTaskDescriptor, final IAllocator allocator) {
        // sanity check.
        if(dstTaskDescriptor == null)
            throw new IllegalArgumentException("dstTaskDescriptor == null");
        if(allocator == null)
            throw new IllegalArgumentException("allocator == null");

        taskManager.getIOManager().connectDataChannel(
                taskDescriptor.taskID,
                dstTaskDescriptor.taskID,
                dstTaskDescriptor.getMachineDescriptor(),
                allocator
        );
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
    public MeasurementManager getMeasurementManager() {
        return measurementManager;
    }

    @Override
    public RecordReader getRecordReader() {
        return recordReader;
    }

    @Override
    public RecordWriter getRecordWriter() {
        return recordWriter;
    }

    @Override
    public ITaskManager getTaskManager() {
        return taskManager;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param userCode
     * @return
     */
    private Class<? extends AbstractTaskInvokeable> implantInvokeableCode(final UserCode userCode) {
        // Try to register the bytecode as a class in the JVM.
        final UserCodeImplanter codeImplanter = new UserCodeImplanter(this.getClass().getClassLoader());
        @SuppressWarnings("unchecked")
        final Class<? extends AbstractTaskInvokeable> userCodeClazz = (Class<? extends AbstractTaskInvokeable>) codeImplanter.implantUserCodeClass(userCode);
        // sanity check.
        if (userCodeClazz == null)
            throw new IllegalArgumentException("userCodeClazz == null");

        return userCodeClazz;
    }

    /**
     * @param invokableClazz
     * @return
     */
    private AbstractTaskInvokeable createInvokeable(final Class<? extends AbstractTaskInvokeable> invokableClazz,
                                            final ITaskDriver taskDriver,
                                            final IDataProducer dataProducer,
                                            final IDataConsumer dataConsumer,
                                            final Logger LOG) {
        try {

            final Constructor<? extends AbstractTaskInvokeable> invokeableCtor =
                    invokableClazz.getConstructor(ITaskDriver.class, IDataProducer.class, IDataConsumer.class, Logger.class);

            final AbstractTaskInvokeable invokeable = invokeableCtor.newInstance(taskDriver, dataProducer, dataConsumer, LOG);

            return invokeable;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return
     */
    private StateMachine.FiniteStateMachine<TaskState, TaskTransition> createTaskFSM() {

        final StateMachine.FiniteStateMachineBuilder<TaskState, TaskTransition> taskFSMBuilder =
                new StateMachine.FiniteStateMachineBuilder<>(TaskState.class, TaskTransition.class, TaskState.ERROR);

        final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM =
                taskFSMBuilder.defineState(TaskState.TASK_STATE_CREATED)
                              .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_INPUTS_CONNECTED)
                              .and()
                              .addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_OUTPUTS_CONNECTED)
                              .defineState(TaskState.TASK_STATE_INPUTS_CONNECTED)
                              .addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                              .defineState(TaskState.TASK_STATE_OUTPUTS_CONNECTED)
                              .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                              .defineState(TaskState.TASK_STATE_READY)
                              .addTransition(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_RUNNING)
                              .defineState(TaskState.TASK_STATE_RUNNING)
                              .addTransition(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_FINISHED)
                              .and()
                              .addTransition(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED)
                              .and()
                              .addTransition(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE)
                              .and()
                              .addTransition(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_PAUSED)
                              // .nestFSM(TaskState.TASK_STATE_RUNNING, operatorFSM)
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

        taskFSM.addGlobalStateListener(new StateMachine.FSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {

                try {
                    LOG.info("CHANGE STATE OF TASK " + taskDescriptor.name + " [" + taskDescriptor.taskID + "] FROM " + previousState + " TO "
                            + state + "  [" + transition.toString() + "]");

                    final IOEvents.TaskControlIOEvent stateUpdate =
                            new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE);

                    stateUpdate.setPayload(state);
                    stateUpdate.setTaskID(taskDescriptor.taskID);
                    stateUpdate.setTopologyID(taskDescriptor.topologyID);

                    taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), stateUpdate);
                } catch (Throwable t) {
                    LOG.error(t.getLocalizedMessage(), t);
                    throw t;
                }
            }
        });

        // error state listener.

        taskFSM.addStateListener(TaskState.ERROR, new StateMachine.FSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                throw new IllegalStateException("task " + taskDescriptor.name + " [" + taskDescriptor.taskID + "] from state " + previousState
                        + " to " + state + " is not defined  [" + transition.toString() + "]");
            }
        });

        // task ready state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_READY, new StateMachine.FSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {

                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
                transitionUpdate.setTaskID(taskDescriptor.taskID);
                transitionUpdate.setTopologyID(taskDescriptor.topologyID);

                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        // task finish state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_FINISHED, new StateMachine.FSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {

                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));
                transitionUpdate.setTaskID(taskDescriptor.taskID);
                transitionUpdate.setTopologyID(taskDescriptor.topologyID);

                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        // task failure state listener.

        taskFSM.addStateListener(TaskState.TASK_STATE_FAILURE, new StateMachine.FSMStateAction<TaskState, TaskTransition>() {

            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {

                final IOEvents.TaskControlIOEvent transitionUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));
                transitionUpdate.setTaskID(taskDescriptor.taskID);
                transitionUpdate.setTopologyID(taskDescriptor.topologyID);

                taskManager.getIOManager().sendEvent(taskManager.getWorkloadManagerMachineDescriptor(), transitionUpdate);
            }
        });

        return taskFSM;
    }
}
