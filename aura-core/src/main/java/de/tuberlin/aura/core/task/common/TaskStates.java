package de.tuberlin.aura.core.task.common;

public final class TaskStates {

    // Disallow instantiation.
    private TaskStates() {
    }

    // ---------------------------------------------------
    // Task States & Task Transitions.
    // ---------------------------------------------------

    /**
     *
     */
    public enum TaskState {

        TASK_STATE_CREATED,

        TASK_STATE_OUTPUTS_CONNECTED,

        TASK_STATE_INPUTS_CONNECTED,

        TASK_STATE_READY,

        TASK_STATE_RUNNING,

        TASK_STATE_PAUSED,

        TASK_STATE_FINISHED,

        TASK_STATE_CANCELED,

        TASK_STATE_FAILURE,

        TASK_STATE_RECOVER,

        ERROR
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

        TASK_TRANSITION_FAIL
    }
}
