package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;

public final class TaskEvents {

	// Disallow instantiation.
	private TaskEvents() {
	}

	/**
     *
     */
	public static final class TaskStateTransitionEvent extends Event {

		public static final String TASK_STATE_TRANSITION_EVENT = "TASK_STATE_TRANSITION_EVENT";

		public TaskStateTransitionEvent(TaskTransition transition) {
			super(TASK_STATE_TRANSITION_EVENT);
			// sanity check.
			if (transition == null)
				throw new IllegalArgumentException("transition == null");

			this.transition = transition;
		}

		public final TaskTransition transition;

		@Override
		public String toString() {
			return (new StringBuilder())
				.append("TaskChangeStateEvent = {")
				.append(" type = " + super.type + ", ")
				.append(" transition = " + transition.toString())
				.append(" }").toString();
		}
	}
}
