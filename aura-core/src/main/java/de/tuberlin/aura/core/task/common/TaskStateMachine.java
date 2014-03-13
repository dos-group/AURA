package de.tuberlin.aura.core.task.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class TaskStateMachine {

    // Disallow instantiation.
    private TaskStateMachine() {
    }

    /**
     *
     */
    public static final Map<TaskState, Map<TaskTransition, TaskState>> TASK_STATE_TRANSITION_MATRIX = buildTaskStateTransitionMatrix();

    /**
     *
     */
    public enum TaskState {

        TASK_STATE_CREATED(false),

        TASK_STATE_OUTPUTS_CONNECTED(false),

        TASK_STATE_INPUTS_CONNECTED(false),

        TASK_STATE_READY(false),

        TASK_STATE_RUNNING(false),

        TASK_STATE_PAUSED(false),

        TASK_STATE_FINISHED(true),

        TASK_STATE_CANCELED(true),

        TASK_STATE_FAILURE(true),

        TASK_STATE_UNDEFINED(true),

        TASK_STATE_RECOVER(false);

        private TaskState(boolean isFinalState) {
            this.isFinalState = isFinalState;
        }

        public final boolean isFinalState;
    }

    /**
     *
     */
    public enum TaskTransition {

        TASK_TRANSITION_INVALID,

        TASK_TRANSITION_INPUTS_CONNECTED,

        TASK_TRANSITION_OUTPUTS_CONNECTED,

        TASK_TRANSITION_RUN,

        TASK_TRANSITION_SUSPEND,

        TASK_TRANSITION_RESUME,

        TASK_TRANSITION_FINISH,

        TASK_TRANSITION_CANCEL,

        TASK_TRANSITION_FAIL;
    }

    /**
     *
     */
    private static Map<TaskState, Map<TaskTransition, TaskState>> buildTaskStateTransitionMatrix() {

        final Map<TaskState, Map<TaskTransition, TaskState>> mtx = new HashMap<TaskState, Map<TaskTransition, TaskState>>();

        final Map<TaskTransition, TaskState> t1 = new HashMap<TaskTransition, TaskState>();

        t1.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_INPUTS_CONNECTED);
        t1.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_OUTPUTS_CONNECTED);
        t1.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t1.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_CREATED, Collections.unmodifiableMap(t1));

        final Map<TaskTransition, TaskState> t2 = new HashMap<TaskTransition, TaskState>();

        t2.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_READY);
        t2.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t2.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_OUTPUTS_CONNECTED, Collections.unmodifiableMap(t2));

        final Map<TaskTransition, TaskState> t3 = new HashMap<TaskTransition, TaskState>();

        t3.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_READY);
        t3.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t3.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_INPUTS_CONNECTED, Collections.unmodifiableMap(t3));

        final Map<TaskTransition, TaskState> t4 = new HashMap<TaskTransition, TaskState>();

        t4.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_RUNNING);
        t4.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t4.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_READY, Collections.unmodifiableMap(t4));

        final Map<TaskTransition, TaskState> t5 = new HashMap<TaskTransition, TaskState>();

        t5.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t5.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t5.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t5.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t5.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_PAUSED);
        t5.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t5.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_FINISHED);
        t5.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED);
        t5.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE);

        mtx.put(TaskState.TASK_STATE_RUNNING, Collections.unmodifiableMap(t5));

        final Map<TaskTransition, TaskState> t6 = new HashMap<TaskTransition, TaskState>();

        t6.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_RUNNING);
        t6.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t6.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED);
        t6.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE);

        mtx.put(TaskState.TASK_STATE_PAUSED, Collections.unmodifiableMap(t6));

        final Map<TaskTransition, TaskState> t7 = new HashMap<TaskTransition, TaskState>();

        t7.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t7.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_FINISHED, Collections.unmodifiableMap(t7));

        final Map<TaskTransition, TaskState> t8 = new HashMap<TaskTransition, TaskState>();

        t8.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t8.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_CANCELED, Collections.unmodifiableMap(t8));

        final Map<TaskTransition, TaskState> t9 = new HashMap<TaskTransition, TaskState>();

        t9.put(TaskTransition.TASK_TRANSITION_INVALID, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_UNDEFINED);
        t9.put(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_UNDEFINED);

        mtx.put(TaskState.TASK_STATE_FAILURE, Collections.unmodifiableMap(t9));

        return Collections.unmodifiableMap(mtx);
    }
}
