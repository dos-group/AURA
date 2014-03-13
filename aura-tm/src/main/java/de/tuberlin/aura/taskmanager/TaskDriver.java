package de.tuberlin.aura.taskmanager;


import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.BlockingBufferQueue;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * The driver provide the lifecycle methods of an deployable aura task.
 * The lifecycle methods are called by the task execution unit.
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

    private final IEventHandler handler;

    private TaskStateMachine.TaskState taskState;

    private final TaskDriverContext driverContext;

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

        this.handler = new TaskEventHandler();

        this.queueManager = QueueManager.newInstance(taskDescriptor.taskID, new BlockingBufferQueue.Factory<IOEvents.DataIOEvent>());

        this.driverContext = new TaskDriverContext(this, managerContext, taskDescriptor, taskBindingDescriptor, this, queueManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TaskDriverContext getTaskDriverContext() {
        return driverContext;
    }

    // ---------------------------------------------------
    // Task Driver Lifecycle.
    // ---------------------------------------------------

    /**
     *
     */
    @Override
    public void startupDriver() {

        taskState = TaskStateMachine.TaskState.TASK_STATE_CREATED;

        this.addEventListener(IOEvents.TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT, handler);

        dataConsumer = new TaskDataConsumer(driverContext);

        dataProducer = new TaskDataProducer(driverContext);

        invokeableClazz = implantInvokeableCode(taskDescriptor.userCode);

        invokeable = createInvokeable(
                invokeableClazz,
                driverContext,
                dataProducer,
                dataConsumer,
                LOG
        );

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

            dispatchEvent(
                    new IOEvents.TaskStateTransitionEvent(
                            taskDescriptor.topologyID,
                            taskDescriptor.taskID,
                            TaskStateMachine.TaskTransition.TASK_TRANSITION_FAIL
                    )
            );

            return;
        }

        // TODO: Wait until all gates are closed?

        dispatchEvent(
                new IOEvents.TaskStateTransitionEvent(
                        taskDescriptor.topologyID,
                        taskDescriptor.taskID,
                        TaskStateMachine.TaskTransition.TASK_TRANSITION_FINISH
                )
        );
    }

    /**
     * @param awaitExhaustion
     */
    @Override
    public void teardownDriver(boolean awaitExhaustion) {

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
        final Class<? extends TaskInvokeable> userCodeClazz =
                (Class<? extends TaskInvokeable>) codeImplanter.implantUserCodeClass(userCode);
        // sanity check.
        if (userCodeClazz == null)
            throw new IllegalArgumentException("userCodeClazz == null");

        return userCodeClazz;
    }

    /**
     * @param transition
     * @return
     */
    private TaskStateMachine.TaskState doTaskStateTransition(final TaskStateMachine.TaskTransition transition) {
        // sanity check.
        if (transition == null)
            throw new IllegalArgumentException("taskTransition == null");

        final Map<TaskStateMachine.TaskTransition, TaskStateMachine.TaskState> transitionsSpace =
                TaskStateMachine.TASK_STATE_TRANSITION_MATRIX.get(taskState);
        final TaskStateMachine.TaskState nextState = transitionsSpace.get(transition);
        taskState = nextState;
        return taskState;
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
                    invokableClazz.getConstructor(
                            TaskDriverContext.class,
                            DataProducer.class,
                            DataConsumer.class,
                            Logger.class
                    );

            final TaskInvokeable invokeable =
                    invokeableCtor.newInstance(
                            driverContext,
                            dataProducer,
                            dataConsumer,
                            LOG
                    );

            // sanity check.
            if (invokeable == null) {
                throw new IllegalStateException("invokeable == null");
            }

            return invokeable;

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    private final class TaskEventHandler extends EventHandler {

        private long timeOfLastStateChange = System.currentTimeMillis();

        @Handle(event = IOEvents.TaskStateTransitionEvent.class)
        private void handleTaskStateTransition(final IOEvents.TaskStateTransitionEvent event) {
            synchronized (taskState) {

                final TaskStateMachine.TaskState oldState = taskState;

                if (taskState == TaskStateMachine.TaskState.TASK_STATE_FINISHED)
                    return;

                if (taskState == TaskStateMachine.TaskState.TASK_STATE_CANCELED)
                    return;

                final TaskStateMachine.TaskState nextState = doTaskStateTransition(event.transition);

                final long currentTime = System.currentTimeMillis();

                final IOEvents.MonitoringEvent.TaskStateUpdate taskStateUpdate =
                        new IOEvents.MonitoringEvent.TaskStateUpdate(
                                taskDescriptor.taskID,
                                taskDescriptor.name,
                                oldState,
                                nextState,
                                event.transition,
                                currentTime - timeOfLastStateChange
                        );

                timeOfLastStateChange = currentTime;

                managerContext.ioManager.sendEvent(managerContext.workloadManagerMachine,
                        new IOEvents.MonitoringEvent(
                                taskDescriptor.topologyID,
                                taskStateUpdate
                        )
                );

                switch (nextState) {

                    case TASK_STATE_CREATED: {
                    }
                    break;
                    case TASK_STATE_INPUTS_CONNECTED: {
                    }
                    break;
                    case TASK_STATE_OUTPUTS_CONNECTED: {
                    }
                    break;
                    case TASK_STATE_READY: {
                    }
                    break;
                    case TASK_STATE_RECOVER: {
                    }
                    break;
                    case TASK_STATE_PAUSED: {
                    }
                    break;
                    case TASK_STATE_RUNNING: {
                    }
                    break;
                    case TASK_STATE_FINISHED: {
                    }
                    break;
                    case TASK_STATE_FAILURE: {
                    }
                    break;
                    case TASK_STATE_CANCELED: {
                    }
                    break;

                    case TASK_STATE_UNDEFINED: {
                        throw new IllegalStateException("task " + taskDescriptor.name + " [" + taskDescriptor.taskID + "] from state " + oldState
                                + " to " + taskState + " is not defined  [" + event.transition.toString() + "]");
                    }
                    default:
                        break;
                }
                LOG.info("CHANGE STATE OF TASK " + taskDescriptor.name + " [" + taskDescriptor.taskID + "] FROM " + oldState + " TO "
                        + taskState + "  [" + event.transition.toString() + "]");
            }
        }
    }
}