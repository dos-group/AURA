package de.tuberlin.aura.core.topology;

public class TopologyStates {

    // Disallow instantiation.
    private TopologyStates() {
    }

    // ---------------------------------------------------
    // Topology States & Topology Transitions.
    // ---------------------------------------------------

    /**
     *
     */
    public static enum TopologyState {

        TOPOLOGY_STATE_CREATED,

        TOPOLOGY_STATE_PARALLELIZED,

        TOPOLOGY_STATE_SCHEDULED,

        TOPOLOGY_STATE_DEPLOYED,

        TOPOLOGY_STATE_RUNNING,

        TOPOLOGY_STATE_FINISHED,

        TOPOLOGY_STATE_CANCELED,

        TOPOLOGY_STATE_FAILURE,

        ERROR
    }

    /**
     *
     */
    public static enum TopologyTransition {

        TOPOLOGY_TRANSITION_PARALLELIZE,

        TOPOLOGY_TRANSITION_SCHEDULE,

        TOPOLOGY_TRANSITION_DEPLOY,

        TOPOLOGY_TRANSITION_RUN,

        TOPOLOGY_TRANSITION_FINISH,

        TOPOLOGY_TRANSITION_CANCEL,

        TOPOLOGY_TRANSITION_FAIL
    }
}
