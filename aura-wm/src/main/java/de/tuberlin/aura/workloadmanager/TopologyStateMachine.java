package de.tuberlin.aura.workloadmanager;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopologyStateMachine {

    // Disallow instantiation.
    private TopologyStateMachine() {}

    /**
     *
     */
    public static final Map<TopologyState,Map<TopologyTransition,TopologyState>> TOPOLOGY_STATE_TRANSITION_MATRIX =
            buildTopologyStateTransitionMatrix();

    /**
     *
     */
    public enum TopologyState {

        TOPOLOGY_STATE_CREATED,

        TOPOLOGY_STATE_PARALLELIZED,

        TOPOLOGY_STATE_SCHEDULED,

        TOPOLOGY_STATE_DEPLOYED,

        TOPOLOGY_STATE_RUNNING,

        TOPOLOGY_STATE_FINISHED,

        TOPOLOGY_STATE_FAILURE,

        TOPOLOGY_STATE_UNDEFINED
    }

    /**
     *
     */
    public enum TopologyTransition {

        TOPOLOGY_TRANSITION_PARALLELIZE,

        TOPOLOGY_TRANSITION_SCHEDULE,

        TOPOLOGY_TRANSITION_DEPLOY,

        TOPOLOGY_TRANSITION_RUN,

        TOPOLOGY_TRANSITION_FINISH,

        TOPOLOGY_TRANSITION_FAIL
    }

    /**
     *
     */
    private static Map<TopologyState,Map<TopologyTransition,TopologyState>> buildTopologyStateTransitionMatrix() {

        final Map<TopologyState,Map<TopologyTransition,TopologyState>> mtx = new HashMap<TopologyState,Map<TopologyTransition,TopologyState>>();

        final Map<TopologyTransition,TopologyState> t1 = new HashMap<TopologyTransition,TopologyState>();

        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_PARALLELIZED );
        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t1.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_CREATED, Collections.unmodifiableMap( t1 ) );

        final Map<TopologyTransition,TopologyState> t2 = new HashMap<TopologyTransition,TopologyState>();

        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_SCHEDULED );
        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t2.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_PARALLELIZED, Collections.unmodifiableMap( t2 ) );

        final Map<TopologyTransition,TopologyState> t3 = new HashMap<TopologyTransition,TopologyState>();

        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_DEPLOYED );
        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t3.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_SCHEDULED, Collections.unmodifiableMap( t3 ) );

        final Map<TopologyTransition,TopologyState> t4 = new HashMap<TopologyTransition,TopologyState>();

        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_RUNNING );
        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t4.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_DEPLOYED, Collections.unmodifiableMap( t4 ) );

        final Map<TopologyTransition,TopologyState> t5 = new HashMap<TopologyTransition,TopologyState>();

        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_FINISHED );
        t5.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_FAILURE );

        mtx.put( TopologyState.TOPOLOGY_STATE_RUNNING, Collections.unmodifiableMap( t5 ) );

        final Map<TopologyTransition,TopologyState> t6 = new HashMap<TopologyTransition,TopologyState>();

        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t6.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_FINISHED, Collections.unmodifiableMap( t6 ) );

        final Map<TopologyTransition,TopologyState> t7 = new HashMap<TopologyTransition,TopologyState>();

        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, 	TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, 		TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_RUN, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_FINISH, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );
        t7.put( TopologyTransition.TOPOLOGY_TRANSITION_FAIL, 			TopologyState.TOPOLOGY_STATE_UNDEFINED );

        mtx.put( TopologyState.TOPOLOGY_STATE_FAILURE, Collections.unmodifiableMap( t7 ) );

        return Collections.unmodifiableMap( mtx );
    }
}
