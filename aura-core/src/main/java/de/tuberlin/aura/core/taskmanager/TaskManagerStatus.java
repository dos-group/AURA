package de.tuberlin.aura.core.taskmanager;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class TaskManagerStatus implements Serializable{

    public final List<ExecutionUnitStatus> executionUnitStatuses;

    public TaskManagerStatus(final List<ExecutionUnitStatus> executionUnitStatuses) {
        // sanity check.
        if (executionUnitStatuses == null)
            throw new IllegalArgumentException("executionUnitStatuses == null");

        this.executionUnitStatuses = Collections.unmodifiableList(executionUnitStatuses);
    }

    public TaskManagerStatus() {
        this(null);
    }

    public static final class ExecutionUnitStatus implements Serializable {

        public final UUID  taskID;

        public final String taskName;

        public ExecutionUnitStatus(final UUID taskID, final String taskName) {

            this.taskID = taskID;

            this.taskName = taskName;
        }

        public ExecutionUnitStatus() {
            this(null, null);
        }
    }
}
