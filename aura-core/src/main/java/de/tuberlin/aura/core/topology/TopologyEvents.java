package de.tuberlin.aura.core.topology;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyTransition;

public class TopologyEvents {

    // Disallow instantiation.
    private TopologyEvents() {}

    /**
    *
    */
    public static final class TopologyStateTransitionEvent extends Event {

        private static final long serialVersionUID = 1L;

        public static final String TOPOLOGY_STATE_TRANSITION_EVENT = "TOPOLOGY_STATE_TRANSITION_EVENT";

        public TopologyStateTransitionEvent(final TopologyTransition transition) {
            super(TOPOLOGY_STATE_TRANSITION_EVENT);
            // sanity check.
            if (transition == null)
                throw new IllegalArgumentException("taskTransition == null");

            this.transition = transition;
        }

        public final TopologyTransition transition;

        @Override
        public String toString() {
            return (new StringBuilder()).append("TopologyStateTransitionEvent = {")
                                        .append(" type = " + super.type + ", ")
                                        .append(" taskTransition = " + transition.toString())
                                        .append(" }")
                                        .toString();
        }
    }
}
