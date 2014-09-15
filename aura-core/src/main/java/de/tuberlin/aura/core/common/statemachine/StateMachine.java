package de.tuberlin.aura.core.common.statemachine;

import java.util.*;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;

public final class StateMachine {

    // Disallow instantiation.
    private StateMachine() {}

    public static final class FiniteStateMachineBuilder<S extends Enum<S>, T extends Enum<T>> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private S initialState;

        private final S errorState;

        private final Class<T> transitionClazz;

        private final Map<S, Map<T, S>> stateTransitionMtx;

        private final TransitionBuilder<S, T> transitionBuilder;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

        private final Map<T, IFSMTransitionConstraint<S, T>> transitionConstraintMap;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public FiniteStateMachineBuilder(final Class<S> stateClazz, final Class<T> transitionClazz, final S errorState) {
            // sanity check.
            if (stateClazz == null)
                throw new IllegalArgumentException("stateClazz == null");
            if (transitionClazz == null)
                throw new IllegalArgumentException("transitionClazz == null");

            this.nestedFSMs = new HashMap<>();

            this.stateTransitionMtx = new HashMap<>();
            // Fill the matrix with all possible states.
            for (final S state : stateClazz.getEnumConstants()) {
                stateTransitionMtx.put(state, null);

                nestedFSMs.put(state, new ArrayList<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>());
            }

            this.initialState = null;

            this.errorState = errorState;

            this.transitionClazz = transitionClazz;

            this.transitionBuilder = new TransitionBuilder<>(this);

            this.finalStates = new HashSet<>();

            this.transitionConstraintMap = new HashMap<>();
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        /*public FiniteStateMachineBuilder<S, T> nestFSM(final S state, final FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>> nestedFSM) {
            // sanity check.
            if (state == null)
                throw new IllegalArgumentException("state == null");
            if (nestedFSM == null)
                throw new IllegalArgumentException("nestedFSM == null");

            final List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>> nestedStateMachines = nestedFSMs.get(state);
            // sanity check.
            if (nestedStateMachines.contains(nestedFSM)) {
                throw new IllegalStateException("FSM exists already");
            }

            nestedStateMachines.add(nestedFSM);
            return this;
        }*/

        public TransitionBuilder<S, T> defineState(final S state) {
            // sanity check.
            if (state == null)
                throw new IllegalArgumentException("state == null");

            if (stateTransitionMtx.get(state) != null)
                throw new IllegalStateException("state already defined");

            final Map<T, S> transitionMap = new HashMap<>();
            for (final T transition : transitionClazz.getEnumConstants()) {
                transitionMap.put(transition, errorState);
            }

            stateTransitionMtx.put(state, transitionMap);

            return transitionBuilder.currentState(state);
        }

        public TransitionBuilder<S, T> and() {
            return transitionBuilder;
        }

        public FiniteStateMachineBuilder<S, T> setInitialState(final S initialState) {
            // sanity check.
            if (initialState == null)
                throw new IllegalArgumentException("initialState == null");
            this.initialState = initialState;
            return this;
        }

        public FiniteStateMachine<S, T> build() {
            // sanity check.
            if (initialState == null)
                throw new IllegalStateException("initialState == null");

            return new FiniteStateMachine<>(Collections.unmodifiableMap(stateTransitionMtx),
                                            initialState,
                                            errorState,
                                            Collections.unmodifiableSet(finalStates),
                                            Collections.unmodifiableMap(nestedFSMs),
                                            transitionClazz,
                                            Collections.unmodifiableMap(transitionConstraintMap));
        }

        // ---------------------------------------------------
        // Inner Classes.
        // ---------------------------------------------------

        public final class TransitionBuilder<S extends Enum<S>, T extends Enum<T>> {

            // ---------------------------------------------------
            // Fields.
            // ---------------------------------------------------

            private final FiniteStateMachineBuilder<S, T> fsmBuilder;

            private S currentState;

            // ---------------------------------------------------
            // Constructors.
            // ---------------------------------------------------

            public TransitionBuilder(final FiniteStateMachineBuilder<S, T> fsmBuilder) {
                // sanity check.
                if (fsmBuilder == null)
                    throw new IllegalArgumentException("fsmBuilder == null");

                this.fsmBuilder = fsmBuilder;
            }

            // ---------------------------------------------------
            // Public Methods.
            // ---------------------------------------------------

            public FiniteStateMachineBuilder<S, T> addTransition(final T transition, final S nextState, final IFSMTransitionConstraint<S, T> constraint) {
                // sanity check.
                if (transition == null)
                    throw new IllegalArgumentException("transition == null");
                if (nextState == null)
                    throw new IllegalArgumentException("nextState == null");

                final Map<T, S> transitionMap = fsmBuilder.stateTransitionMtx.get(currentState);

                if (transitionMap.get(transition) != errorState) {
                    throw new IllegalStateException("transition already defined");
                }

                transitionMap.put(transition, nextState);

                if (constraint != null)
                    fsmBuilder.transitionConstraintMap.put(transition, constraint);

                return fsmBuilder;
            }

            public FiniteStateMachineBuilder<S, T> addTransition(final T transition, final S nextState) {
                return addTransition(transition, nextState, null);
            }

            public FiniteStateMachineBuilder<S, T> noTransition() {
                // sanity check.
                if (fsmBuilder.finalStates.contains(currentState))
                    throw new IllegalStateException();
                fsmBuilder.finalStates.add(currentState);
                return fsmBuilder;
            }

            // ---------------------------------------------------
            // Private Methods.
            // ---------------------------------------------------

            private TransitionBuilder<S, T> currentState(final S state) {
                // sanity check.
                if (state == null)
                    throw new IllegalArgumentException("state == null");
                this.currentState = state;
                return this;
            }
        }
    }

    public static final class FSMStateEvent<S, T> extends Event {

        // ---------------------------------------------------
        // Constants.
        // ---------------------------------------------------

        public static final String FSM_STATE_EVENT_PREFIX = "FSM_STATE_EVENT_";

        public static final String FSM_STATE_CHANGE = "FSM_STATE_CHANGE_EVENT";

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final S previousState;

        public final T transition;

        public final S state;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public FSMStateEvent(final S previousState, final T transition, final S state) {
            super(FSM_STATE_EVENT_PREFIX + state.toString());

            this.previousState = previousState;

            this.transition = transition;

            this.state = state;
        }

        public FSMStateEvent(final String type, final S previousState, final T transition, final S state) {
            super(type);

            this.previousState = previousState;

            this.transition = transition;

            this.state = state;
        }
    }

    public static interface IFSMStateAction<S, T> {

        public abstract void stateAction(final S previousState, final T transition, final S state);
    }

    public static class FSMTransitionEvent<T extends Enum<T>> extends IOEvents.ControlIOEvent {

        public static final String FSMTransition = "FSM_TRANSITION_";

        public FSMTransitionEvent(final T transition) {
            super(FSMTransition + transition.toString());
            setPayload(transition);
        }
    }

    public static interface IFSMTransitionConstraint<S extends Enum<S>, T extends Enum<T>> {

        public abstract void defineTransitionConstraint(final T transition, final FiniteStateMachine<S, T> stateMachine);
    }

    public static abstract class FSMTransitionConstraint2<S extends Enum<S>, T extends Enum<T>> implements IFSMTransitionConstraint<S, T> {

        private final Enum<?> domainTransition;

        public FSMTransitionConstraint2(final Enum<?> domainTransition) {
            // sanity check.
            if (domainTransition == null)
                throw new IllegalArgumentException("domainTransition == null");

            this.domainTransition = domainTransition;
        }

        public abstract boolean eval(final FSMTransitionEvent<? extends Enum<?>> event);

        public void defineTransitionConstraint(final T hostTransition, final FiniteStateMachine<S, T> stateMachine) {
            stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + domainTransition.toString(), new IEventHandler() {

                @Override
                public void handleEvent(Event event) {
                    if (eval((FSMTransitionEvent<? extends Enum<?>>) event)) {

                        // TODO:  HERE IS THE BUG !

                        stateMachine.dispatchEvent(new FSMTransitionEvent<>(hostTransition));
                    }
                }
            });
        }
    }

    public static final class FiniteStateMachine<S extends Enum<S>, T extends Enum<T>> extends EventDispatcher {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private S currentState;

        private final Map<S, Map<T, S>> stateTransitionMtx;

        private final S errorState;

        private final S initialState;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

        private final Class<T> transitionClazz;

        private final Map<T, IFSMTransitionConstraint<S, T>> transitionConstraintMap;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public FiniteStateMachine(final Map<S, Map<T, S>> stateTransitionMtx,
                                  final S initialState,
                                  final S errorState,
                                  final Set<S> finalStates,
                                  final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs,
                                  final Class<T> transitionClazz,
                                  final Map<T, IFSMTransitionConstraint<S, T>> transitionConstraintMap) {

            // The EventDispatcher must run a own dispatch thread, because of
            // nested dispatch that can occur (e.g. in transition constraints).
            // Otherwise the calling thread of the dispatch method would
            // encounter a deadlock.
            super(true);

            // sanity check.
            if (stateTransitionMtx == null)
                throw new IllegalArgumentException("stateTransitionMtx == null");
            if (initialState == null)
                throw new IllegalArgumentException("initialState == null");
            if (errorState == null)
                throw new IllegalArgumentException("errorState == null");
            if (finalStates == null)
                throw new IllegalArgumentException("finalStates == null");
            if (nestedFSMs == null)
                throw new IllegalArgumentException("nestedFSMs == null");
            if (transitionClazz == null)
                throw new IllegalArgumentException("transitionClazz == null");
            if (transitionConstraintMap == null)
                throw new IllegalArgumentException("transitionConstraintMap == null");

            this.stateTransitionMtx = stateTransitionMtx;

            this.currentState = initialState;

            this.initialState = initialState;

            this.errorState = errorState;

            this.finalStates = finalStates;

            this.nestedFSMs = nestedFSMs;

            this.transitionClazz = transitionClazz;

            this.transitionConstraintMap = transitionConstraintMap;

            for (final T transition : transitionClazz.getEnumConstants()) {
                this.addEventListener(FSMTransitionEvent.FSMTransition + transition.toString(), new IEventHandler() {

                    @Override
                    public void handleEvent(Event event) {
                        doTransition(event.getPayload());
                    }
                });
            }

            for (List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>> nestedFSMList : nestedFSMs.values()) {
                for (final FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>> nestedFSM : nestedFSMList) {
                    for (final Object transition : nestedFSM.transitionClazz.getEnumConstants()) {
                        this.addEventListener(FSMTransitionEvent.FSMTransition + transition.toString(), new IEventHandler() {

                            @Override
                            public void handleEvent(Event event) {
                                nestedFSM.doTransition(event.getPayload());
                            }
                        });
                    }
                }
            }

            for (final Map.Entry<T, IFSMTransitionConstraint<S, T>> entry : this.transitionConstraintMap.entrySet()) {
                entry.getValue().defineTransitionConstraint(entry.getKey(), this);
            }
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addStateListener(final S state, final IFSMStateAction<S, T> stateAction) {
            // sanity check.
            if (state == null)
                throw new IllegalArgumentException("state == null");

            this.addEventListener(FSMStateEvent.FSM_STATE_EVENT_PREFIX + state.toString(), new IEventHandler() {

                @Override
                @SuppressWarnings("unchecked")
                public void handleEvent(Event e) {
                    final FSMStateEvent<S, T> event = (FSMStateEvent<S, T>) e;
                    stateAction.stateAction(event.previousState, event.transition, event.state);
                }

                @Override
                public String toString() {
                    return stateAction.toString();
                }
            });
        }

        public void addGlobalStateListener(final IFSMStateAction<S, T> stateAction) {
            this.addEventListener(FSMStateEvent.FSM_STATE_CHANGE, new IEventHandler() {

                @Override
                public void handleEvent(Event e) {
                    @SuppressWarnings("unchecked")
                    final FSMStateEvent<S, T> event = (FSMStateEvent<S, T>) e;
                    stateAction.stateAction(event.previousState, event.transition, event.state);
                }
            });
        }

        public void start() {
            dispatchEvent(new FSMStateEvent<S, T>(null, null, currentState));
        }

        public synchronized void doTransition(final Object transitionObj) {
            // sanity check.
            if (transitionObj == null)
                throw new IllegalArgumentException("transition == null ");
            if (transitionObj.getClass() != transitionClazz)
                throw new IllegalStateException();

            @SuppressWarnings("unchecked")
            final T transition = (T) transitionObj;

            if (finalStates.contains(currentState)) {
                return;
                // throw new IllegalStateException(currentState +
                // " is a final state, no transition allowed");
            }

            final Map<T, S> transitionSpace = stateTransitionMtx.get(currentState);
            final S nextState = transitionSpace.get(transition);

            dispatchEvent(new FSMStateEvent<>(currentState, transition, nextState));

            dispatchEvent(new FSMStateEvent<>(FSMStateEvent.FSM_STATE_CHANGE, currentState, transition, nextState));

            for (final FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>> nestedFSM : nestedFSMs.get(nextState)) {
                nestedFSM.start();
            }

            currentState = nextState;
        }

        public boolean isInFinalState() {
            return finalStates.contains(currentState);
        }

        public void reset() {
            currentState = initialState;
        }

        public S getCurrentState() {
            return currentState;
        }
    }
}
