package de.tuberlin.aura.core.common.statemachine;

import com.sun.prism.impl.ps.BaseShaderContext;

import java.util.*;

public final class StateMachine {

    // Disallow instantiation.
    private StateMachine() {
    }

    public final class FiniteStateMachineBuilder<T extends Enum<T>,S extends Enum<S>> {

        public final class TransitionBuilder<T extends Enum<T>,S extends Enum<S>> {

            public TransitionBuilder(final FiniteStateMachineBuilder<T,S> fsmBuilder) {
                // Sanity check.
                if(transitionClazz == null)
                    throw new IllegalArgumentException("transitionClazz == null");

                this.fsmBuilder = fsmBuilder;

                this.transitions = new HashSet<>();
            }

            private final FiniteStateMachineBuilder<T,S> fsmBuilder;

            private final Set<T> transitions;

            private S currentState;

            public TransitionBuilder<T,S> currentState(final S state) {
                // Sanity check.
                if(state == null)
                    throw new IllegalArgumentException("state == null");

                this.currentState = state;
                return this;
            }

            public FiniteStateMachineBuilder<T,S> addTransition(final T transition) {
                // Sanity check.
                if(transition == null)
                    throw new IllegalArgumentException("transition == null");

                transitions.add(transition);
                return fsmBuilder;
            }

            public Set<T> returnTransitions() {
                return Collections.unmodifiableSet(transitions);
            }
        }

        private final Class<S> stateClazz;

        private final Class<T> transitionClazz;

        private final Map<T,S> stateTransitionMtx;

        //private final Set<S> initialStates;

        //private final Set<S> finalStates;

        private final TransitionBuilder<T,S> transitionBuilder;

        public FiniteStateMachineBuilder(final Class<T> transitionClazz, final Class<S> stateClazz) {
            // Sanity check.
            if(transitionClazz == null)
                throw new IllegalArgumentException("transitionClazz == null");
            if(stateClazz == null)
                throw new IllegalArgumentException("stateClazz == null");

            this.transitionClazz = transitionClazz;

            this.stateClazz = stateClazz;

            this.stateTransitionMtx = new HashMap<>();

            //this.initialStates = new HashSet<>();

            //this.finalStates = new HashSet<>();

            this.transitionBuilder = new TransitionBuilder<>(this);
        }

        public TransitionBuilder<T,S> addState(final S state) {
            // Sanity check.
            if(state == null)
                throw new IllegalArgumentException("state == null");



            return null;
        }
    }

    public final class FiniteStateMachine<S,T> {

        private final Map<T,S> stateTransitionMtx;

        public FiniteStateMachine(final Map<T,S> stateTransitionMtx) {
            // Sanity check.
            if(stateTransitionMtx == null)
                throw new IllegalArgumentException("stateTransitionMtx == null");

            this.stateTransitionMtx = stateTransitionMtx;
        }

    }

}
