package de.tuberlin.aura.core.topology;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.common.TaskStateMachine;

/**
 * TODO: We need a clean and safe PAXOS implementation! This here is something like a two-phase
 * commit.
 */
public final class TaskStateConsensus {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskStateConsensus(final EnumSet<TaskStateMachine.TaskState> positiveStates,
                              final EnumSet<TaskStateMachine.TaskState> negativeStates,
                              final AuraDirectedGraph.AuraTopology topology) {
        // sanity check.
        if (positiveStates == null)
            throw new IllegalArgumentException("consensusState == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        this.positiveStates = positiveStates;

        this.negativeStates = negativeStates;

        this.taskIDs = new HashSet<UUID>(topology.executionNodeMap.keySet());

        this.isConsensus = true;

        this.isVotingActive = true;
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Set<UUID> taskIDs;

    private final EnumSet<TaskStateMachine.TaskState> positiveStates;

    private final EnumSet<TaskStateMachine.TaskState> negativeStates;

    private boolean isConsensus;

    private boolean isVotingActive;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public synchronized boolean voteSuccess(final IOEvents.MonitoringEvent.TaskStateUpdate stateUpdate) {

        if (!isVotingActive)
            return false;

        if (!positiveStates.contains(stateUpdate.nextTaskState) && (negativeStates != null && !negativeStates.contains(stateUpdate.nextTaskState)))
            return false;

        if (taskIDs.size() > 0) {

            if (positiveStates.contains(stateUpdate.nextTaskState))
                isConsensus &= taskIDs.remove(stateUpdate.taskID);

            if (taskIDs.size() == 0) {
                isVotingActive = false;
                return isConsensus;
            }
        }

        return false;
    }

    public boolean voteFail(final IOEvents.MonitoringEvent.TaskStateUpdate stateUpdate) {

        final boolean result = isVotingActive && negativeStates != null && negativeStates.contains(stateUpdate.nextTaskState);

        if (result) {
            isConsensus = isVotingActive = false;
        }

        return result;
    }

    public boolean consensus() {
        return isConsensus;
    }
}
