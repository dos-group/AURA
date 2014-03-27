package de.tuberlin.aura.taskmanager;


import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BlockingBufferQueue;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.task.common.TaskStates.TaskState;
import de.tuberlin.aura.core.task.common.TaskStates.TaskTransition;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;

/**
 *
 */
public final class TaskDriver extends EventDispatcher implements TaskDriverLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskDriver.class);

    private final TaskManagerContext managerContext;

    private final Descriptors.TaskDescriptor taskDescriptor;

    private final Descriptors.TaskBindingDescriptor taskBindingDescriptor;

    private final QueueManager<IOEvents.DataIOEvent> queueManager;

    private DataProducer dataProducer;

    private DataConsumer dataConsumer;

    private Class<? extends TaskInvokeable> invokeableClazz;

    private TaskInvokeable invokeable;

    private final TaskDriverContext driverContext;

    private final StateMachine.FiniteStateMachine<TaskState, TaskTransition> taskFSM;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskDriver(final TaskManagerContext managerContext, final Descriptors.TaskDeploymentDescriptor deploymentDescriptor) {
        super(true);

        // sanity check.
        if (managerContext == null)
            throw new IllegalArgumentException("managerContext == null");
        if (deploymentDescriptor == null)
            throw new IllegalArgumentException("deploymentDescriptor == null");

        this.managerContext = managerContext;

        this.taskDescriptor = deploymentDescriptor.taskDescriptor;

        this.taskBindingDescriptor = deploymentDescriptor.taskBindingDescriptor;

        this.taskFSM = createTaskFSM();

        this.queueManager = QueueManager.newInstance(taskDescriptor.taskID, new BlockingBufferQueue.Factory<IOEvents.DataIOEvent>());

        this.driverContext = new TaskDriverContext(this, managerContext, taskDescriptor, taskBindingDescriptor, this, queueManager, taskFSM);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TaskDriverContext getTaskDriverContext() {
        return driverContext;
    }

    // ------------- Task Driver Lifecycle ---------------

    /**
     *
     */
    @Override
    public void startupDriver(final MemoryManager.Allocator inputAllocator, final MemoryManager.Allocator outputAllocator) {
        // sanity check.
        if (inputAllocator == null)
            throw new IllegalArgumentException("inputAllocator == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        dataConsumer = new TaskDataConsumer(driverContext, inputAllocator);

        driverContext.setDataConsumer(dataConsumer);

        dataProducer = new TaskDataProducer(driverContext, outputAllocator);

        driverContext.setDataProducer(dataProducer);

        invokeableClazz = implantInvokeableCode(taskDescriptor.userCode);

        invokeable = createInvokeable(invokeableClazz, driverContext, dataProducer, dataConsumer, LOG);

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

            LOG.error(t.getLocalizedMessage());

            taskFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FAIL));

            return;
        }

        // TODO: Wait until all gates are closed?

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
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param userCode
     * @return
     */
    private Class<? extends TaskInvokeable> implantInvokeableCode(final UserCode userCode) {
        // Try to register the bytecode as a class in the JVM.
        final UserCodeImplanter codeImplanter = new UserCodeImplanter(this.getClass().getClassLoader());
        @SuppressWarnings("unchecked")
        final Class<? extends TaskInvokeable> userCodeClazz = (Class<? extends TaskInvokeable>) codeImplanter.implantUserCodeClass(userCode);
        // sanity check.
        if (userCodeClazz == null)
            throw new IllegalArgumentException("userCodeClazz == null");

        return userCodeClazz;
    }

    /**
     * @param invokableClazz
     * @return
     */
    private TaskInvokeable createInvokeable(final Class<? extends TaskInvokeable> invokableClazz,
                                            final TaskDriverContext driverContext,
                                            final DataProducer dataProducer,
                                            final DataConsumer dataConsumer,
                                            final Logger LOG) {
        try {

            final Constructor<? extends TaskInvokeable> invokeableCtor =
                    invokableClazz.getConstructor(TaskDriverContext.class, DataProducer.class, DataConsumer.class, Logger.class);

            final TaskInvokeable invokeable = invokeableCtor.newInstance(driverContext, dataProducer, dataConsumer, LOG);

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

                final IOEvents.TaskControlIOEvent stateUpdate =
                        new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE);

                stateUpdate.setPayload(state);
                stateUpdate.setTaskID(taskDescriptor.taskID);
                stateUpdate.setTopologyID(taskDescriptor.topologyID);

                managerContext.ioManager.sendEvent(managerContext.workloadManagerMachine, stateUpdate);

                LOG.info("CHANGE STATE OF TASK " + taskDescriptor.name + " [" + taskDescriptor.taskID + "] FROM " + previousState + " TO " + state
                        + "  [" + transition.toString() + "]");
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

                managerContext.ioManager.sendEvent(managerContext.workloadManagerMachine, transitionUpdate);
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

                managerContext.ioManager.sendEvent(managerContext.workloadManagerMachine, transitionUpdate);
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

                managerContext.ioManager.sendEvent(managerContext.workloadManagerMachine, transitionUpdate);
            }
        });

        return taskFSM;
    }
}
