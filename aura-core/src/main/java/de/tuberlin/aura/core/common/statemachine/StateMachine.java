package de.tuberlin.aura.core.common.statemachine;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;

public final class StateMachine {

    // Disallow instantiation.
    private StateMachine() {}

    /**
     * @param <S>
     * @param <T>
     */
    public static final class FiniteStateMachineBuilder<S extends Enum<S>, T extends Enum<T>> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        /**
         * Logger.
         */
        private static final Logger LOG = LoggerFactory.getLogger(FiniteStateMachine.class);

        private S initialState;

        private final S errorState;

        private final Class<T> transitionClazz;

        private final Map<S, Map<T, S>> stateTransitionMtx;

        private final TransitionBuilder<S, T> transitionBuilder;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

        private final Map<T, FSMTransitionConstraint<S, T>> transitionConstraintMap;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        /**
         * @param stateClazz
         * @param transitionClazz
         * @param errorState
         */
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

        /**
         * @param state
         * @param nestedFSM
         * @return
         */
        public FiniteStateMachineBuilder<S, T> nestFSM(final S state, final FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>> nestedFSM) {
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
        }

        /**
         * @param state
         * @return
         */
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

        /**
         * @return
         */
        public TransitionBuilder<S, T> and() {
            return transitionBuilder;
        }

        /**
         * @param initialState
         * @return
         */
        public FiniteStateMachineBuilder<S, T> setInitialState(final S initialState) {
            // sanity check.
            if (initialState == null)
                throw new IllegalArgumentException("initialState == null");
            this.initialState = initialState;
            return this;
        }

        /**
         * @return
         */
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

        /**
         * @param <S>
         * @param <T>
         */
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

            /**
             * @param transition
             * @param nextState
             * @param constraint
             * @return
             */
            public FiniteStateMachineBuilder<S, T> addTransition(final T transition, final S nextState, final FSMTransitionConstraint<S, T> constraint) {
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

            /**
             * @param transition
             * @param nextState
             * @return
             */
            public FiniteStateMachineBuilder<S, T> addTransition(final T transition, final S nextState) {
                return addTransition(transition, nextState, null);
            }

            /**
             * @return
             */
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

    /**
     * @param <S>
     * @param <T>
     */
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

        /**
         * @param previousState
         * @param transition
         * @param state
         */
        public FSMStateEvent(final S previousState, final T transition, final S state) {
            super(FSM_STATE_EVENT_PREFIX + state.toString());

            this.previousState = previousState;

            this.transition = transition;

            this.state = state;
        }

        /**
         * @param type
         * @param previousState
         * @param transition
         * @param state
         */
        public FSMStateEvent(final String type, final S previousState, final T transition, final S state) {
            super(type);

            this.previousState = previousState;

            this.transition = transition;

            this.state = state;
        }
    }

    /**
     * @param <S>
     * @param <T>
     */
    public static interface FSMStateAction<S, T> {

        /**
         * @param previousState
         * @param transition
         * @param state
         */
        public abstract void stateAction(final S previousState, final T transition, final S state);
    }

    /**
     * @param <T>
     */
    public static class FSMTransitionEvent<T extends Enum<T>> extends IOEvents.ControlIOEvent {

        public static final String FSMTransition = "FSM_TRANSITION_";

        /**
         * @param transition
         */
        public FSMTransitionEvent(final T transition) {
            super(FSMTransition + transition.toString());
            setPayload(transition);
        }
    }

    /**
     * @param <S>
     * @param <T>
     */
    public static interface FSMTransitionConstraint<S extends Enum<S>, T extends Enum<T>> {

        /**
         * @param transition
         * @param stateMachine
         */
        public abstract void defineTransitionConstraint(final T transition, final FiniteStateMachine<S, T> stateMachine);
    }

    /**
     * @param <S>
     * @param <T>
     */
    public static abstract class FSMTransitionConstraint2<S extends Enum<S>, T extends Enum<T>> implements FSMTransitionConstraint<S, T> {

        private final Enum<?> domainTransition;

        /**
         * @param domainTransition
         */
        public FSMTransitionConstraint2(final Enum<?> domainTransition) {
            // sanity check.
            if (domainTransition == null)
                throw new IllegalArgumentException("domainTransition == null");

            this.domainTransition = domainTransition;
        }

        /**
         * @param event
         * @return
         */
        public abstract boolean eval(final FSMTransitionEvent<? extends Enum<?>> event);

        /**
         * @param hostTransition
         * @param stateMachine
         */
        public void defineTransitionConstraint(final T hostTransition, final FiniteStateMachine<S, T> stateMachine) {
            stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + domainTransition.toString(), new IEventHandler() {

                @Override
                public void handleEvent(Event event) {
                    if (eval((FSMTransitionEvent<? extends Enum<?>>) event)) {
                        stateMachine.dispatchEvent(new FSMTransitionEvent<>(hostTransition));
                    }
                }
            });
        }
    }

    /**
     * @param <S>
     * @param <T>
     */
    public static final class FiniteStateMachine<S extends Enum<S>, T extends Enum<T>> extends EventDispatcher {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        /**
         * Logger.
         */
        private static final Logger LOG = LoggerFactory.getLogger(FiniteStateMachine.class);

        private S currentState;

        private final Map<S, Map<T, S>> stateTransitionMtx;

        private final S errorState;

        private final S initialState;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

        private final Class<T> transitionClazz;

        private final Map<T, FSMTransitionConstraint<S, T>> transitionConstraintMap;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        /**
         * @param stateTransitionMtx
         * @param initialState
         * @param errorState
         * @param finalStates
         * @param nestedFSMs
         * @param transitionClazz
         * @param transitionConstraintMap
         */
        public FiniteStateMachine(final Map<S, Map<T, S>> stateTransitionMtx,
                                  final S initialState,
                                  final S errorState,
                                  final Set<S> finalStates,
                                  final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs,
                                  final Class<T> transitionClazz,
                                  final Map<T, FSMTransitionConstraint<S, T>> transitionConstraintMap) {

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

            for (final Map.Entry<T, FSMTransitionConstraint<S, T>> entry : this.transitionConstraintMap.entrySet()) {
                entry.getValue().defineTransitionConstraint(entry.getKey(), this);
            }
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        /**
         * @param state
         * @param stateAction
         */
        public void addStateListener(final S state, final FSMStateAction<S, T> stateAction) {
            // sanity check.
            if (state == null)
                throw new IllegalArgumentException("state == null");

            this.addEventListener(FSMStateEvent.FSM_STATE_EVENT_PREFIX + state.toString(), new IEventHandler() {

                @Override
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

        /**
         * @param stateAction
         */
        public void addGlobalStateListener(final FSMStateAction<S, T> stateAction) {
            this.addEventListener(FSMStateEvent.FSM_STATE_CHANGE, new IEventHandler() {

                @Override
                public void handleEvent(Event e) {
                    @SuppressWarnings("unchecked")
                    final FSMStateEvent<S, T> event = (FSMStateEvent<S, T>) e;

                    stateAction.stateAction(event.previousState, event.transition, event.state);
                }
            });
        }

        /**
         *
         */
        public void start() {
            dispatchEvent(new FSMStateEvent<S, T>(null, null, currentState));
        }

        /**
         * @param transitionObj
         */
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

        /**
         *
         * @return
         */
        public boolean isInFinalState() {
            return finalStates.contains(currentState);
        }

        /**
         *
         */
        public void reset() {
            currentState = initialState;
        }
    }

    // --------------------- TESTING ---------------------


    // ---------------------------------------------------
    // Topology State Machine.
    // ---------------------------------------------------

    /*
     * public static enum TopologyState {
     * 
     * TOPOLOGY_STATE_CREATED,
     * 
     * TOPOLOGY_STATE_PARALLELIZED,
     * 
     * TOPOLOGY_STATE_SCHEDULED,
     * 
     * TOPOLOGY_STATE_DEPLOYED,
     * 
     * TOPOLOGY_STATE_RUNNING,
     * 
     * TOPOLOGY_STATE_FINISHED,
     * 
     * TOPOLOGY_STATE_CANCELED,
     * 
     * TOPOLOGY_STATE_FAILURE,
     * 
     * ERROR }
     * 
     * public static enum TopologyTransition {
     * 
     * TOPOLOGY_TRANSITION_PARALLELIZE,
     * 
     * TOPOLOGY_TRANSITION_SCHEDULE,
     * 
     * TOPOLOGY_TRANSITION_DEPLOY,
     * 
     * TOPOLOGY_TRANSITION_RUN,
     * 
     * TOPOLOGY_TRANSITION_FINISH,
     * 
     * TOPOLOGY_TRANSITION_CANCEL,
     * 
     * TOPOLOGY_TRANSITION_FAIL }
     * 
     * // --------------------------------------------------- // Task State Machine. //
     * ---------------------------------------------------
     * 
     * public enum TaskState {
     * 
     * TASK_STATE_CREATED,
     * 
     * TASK_STATE_OUTPUTS_CONNECTED,
     * 
     * TASK_STATE_INPUTS_CONNECTED,
     * 
     * TASK_STATE_READY,
     * 
     * TASK_STATE_RUNNING,
     * 
     * TASK_STATE_PAUSED,
     * 
     * TASK_STATE_FINISHED,
     * 
     * TASK_STATE_CANCELED,
     * 
     * TASK_STATE_FAILURE,
     * 
     * TASK_STATE_RECOVER,
     * 
     * ERROR }
     * 
     * public enum TaskTransition {
     * 
     * TASK_TRANSITION_INVALID,
     * 
     * TASK_TRANSITION_INPUTS_CONNECTED,
     * 
     * TASK_TRANSITION_OUTPUTS_CONNECTED,
     * 
     * TASK_TRANSITION_RUN,
     * 
     * TASK_TRANSITION_SUSPEND,
     * 
     * TASK_TRANSITION_RESUME,
     * 
     * TASK_TRANSITION_FINISH,
     * 
     * TASK_TRANSITION_CANCEL,
     * 
     * TASK_TRANSITION_FAIL }
     * 
     * // --------------------------------------------------- // Operator State Machine. //
     * ---------------------------------------------------
     * 
     * public static enum OperatorState {
     * 
     * OPERATOR_STATE_CREATED,
     * 
     * OPERATOR_STATE_OPENED,
     * 
     * OPERATOR_STATE_RUNNING,
     * 
     * OPERATOR_STATE_CLOSED,
     * 
     * OPERATOR_STATE_RELEASED,
     * 
     * ERROR }
     * 
     * public static enum OperatorTransition {
     * 
     * OPERATOR_TRANSITION_OPEN,
     * 
     * OPERATOR_TRANSITION_RUN,
     * 
     * OPERATOR_TRANSITION_CLOSE,
     * 
     * OPERATOR_TRANSITION_RESET,
     * 
     * OPERATOR_TRANSITION_RELEASE }
     * 
     * // --------------------------------------------------- // Entry Point. //
     * ---------------------------------------------------
     * 
     * public static void main(final String[] args) {
     * 
     * // ---------------------------------------------------
     * 
     * final FSMStateAction<OperatorState, OperatorTransition> operatorAction = new
     * FSMStateAction<OperatorState, OperatorTransition>() {
     * 
     * @Override public void stateAction(OperatorState previousState, OperatorTransition transition,
     * OperatorState state) { System.out.println("previousState = " + previousState +
     * " - transition = " + transition + " - state = " + state); } };
     * 
     * final FiniteStateMachineBuilder<OperatorState, OperatorTransition> operatorFSMBuilder = new
     * FiniteStateMachineBuilder<>(OperatorState.class, OperatorTransition.class,
     * OperatorState.ERROR);
     * 
     * final FiniteStateMachine<OperatorState, OperatorTransition> operatorFSM = operatorFSMBuilder
     * .defineState(OperatorState.OPERATOR_STATE_CREATED)
     * .addTransition(OperatorTransition.OPERATOR_TRANSITION_OPEN,
     * OperatorState.OPERATOR_STATE_OPENED) .defineState(OperatorState.OPERATOR_STATE_OPENED)
     * .addTransition(OperatorTransition.OPERATOR_TRANSITION_RUN,
     * OperatorState.OPERATOR_STATE_RUNNING) .defineState(OperatorState.OPERATOR_STATE_RUNNING)
     * .addTransition(OperatorTransition.OPERATOR_TRANSITION_CLOSE,
     * OperatorState.OPERATOR_STATE_CLOSED) .defineState(OperatorState.OPERATOR_STATE_CLOSED)
     * .addTransition(OperatorTransition.OPERATOR_TRANSITION_RELEASE,
     * OperatorState.OPERATOR_STATE_RELEASED)
     * .and().addTransition(OperatorTransition.OPERATOR_TRANSITION_RESET,
     * OperatorState.OPERATOR_STATE_OPENED) .defineState(OperatorState.OPERATOR_STATE_RELEASED)
     * .noTransition() .setInitialState(OperatorState.OPERATOR_STATE_CREATED) .build();
     * 
     * // ---------------------------------------------------
     * 
     * final FSMStateAction<TaskState, TaskTransition> taskAction = new FSMStateAction<TaskState,
     * TaskTransition>() {
     * 
     * @Override public void stateAction(TaskState previousState, TaskTransition transition,
     * TaskState state) { System.out.println("previousState = " + previousState + " - transition = "
     * + transition + " - state = " + state); } }; final FiniteStateMachineBuilder<TaskState,
     * TaskTransition> taskFSMBuilder = new FiniteStateMachineBuilder<>(TaskState.class,
     * TaskTransition.class, TaskState.ERROR);
     * 
     * taskFSMBuilder .defineState(TaskState.TASK_STATE_CREATED)
     * .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED,
     * TaskState.TASK_STATE_INPUTS_CONNECTED)
     * .and().addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED,
     * TaskState.TASK_STATE_OUTPUTS_CONNECTED) .defineState(TaskState.TASK_STATE_INPUTS_CONNECTED)
     * .addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_READY)
     * .defineState(TaskState.TASK_STATE_OUTPUTS_CONNECTED)
     * .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_READY)
     * .defineState(TaskState.TASK_STATE_READY) .addTransition(TaskTransition.TASK_TRANSITION_RUN,
     * TaskState.TASK_STATE_RUNNING) .defineState(TaskState.TASK_STATE_RUNNING)
     * .addTransition(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_FINISHED)
     * .and().addTransition(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED)
     * .and().addTransition(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE)
     * .and().addTransition(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_PAUSED)
     * //.nestFSM(TaskState.TASK_STATE_RUNNING, operatorFSM)
     * .defineState(TaskState.TASK_STATE_FINISHED) .noTransition()
     * .defineState(TaskState.TASK_STATE_CANCELED) .noTransition()
     * .defineState(TaskState.TASK_STATE_FAILURE) .noTransition()
     * .defineState(TaskState.TASK_STATE_PAUSED)
     * .addTransition(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_RUNNING)
     * .setInitialState(TaskState.TASK_STATE_CREATED);
     * 
     * // ---------------------------------------------------
     * 
     * final FSMStateAction<TopologyState, TopologyTransition> topologyAction = new
     * FSMStateAction<TopologyState, TopologyTransition>() {
     * 
     * @Override public void stateAction(TopologyState previousState, TopologyTransition transition,
     * TopologyState state) { System.out.println("previousState = " + previousState +
     * " - transition = " + transition + " - state = " + state); } };
     * 
     * final FSMTransitionConstraint2<TopologyState, TopologyTransition> runTransitionConstraint =
     * new FSMTransitionConstraint2<TopologyState,
     * TopologyTransition>(TaskTransition.TASK_TRANSITION_RUN) {
     * 
     * int counter = 5;
     * 
     * @Override public boolean eval(FSMTransitionEvent<? extends Enum<?>> event) { return
     * (--counter) == 0; } };
     * 
     * final FiniteStateMachineBuilder<TopologyState, TopologyTransition> topologyFSMBuilder = new
     * FiniteStateMachineBuilder<>(TopologyState.class, TopologyTransition.class,
     * TopologyState.ERROR);
     * 
     * final FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM = topologyFSMBuilder
     * .defineState(TopologyState.TOPOLOGY_STATE_CREATED)
     * .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE,
     * TopologyState.TOPOLOGY_STATE_PARALLELIZED)
     * .defineState(TopologyState.TOPOLOGY_STATE_PARALLELIZED)
     * .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE,
     * TopologyState.TOPOLOGY_STATE_SCHEDULED) .defineState(TopologyState.TOPOLOGY_STATE_SCHEDULED)
     * .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY,
     * TopologyState.TOPOLOGY_STATE_DEPLOYED) .defineState(TopologyState.TOPOLOGY_STATE_DEPLOYED)
     * .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_RUN,
     * TopologyState.TOPOLOGY_STATE_RUNNING, runTransitionConstraint)
     * .defineState(TopologyState.TOPOLOGY_STATE_RUNNING)
     * .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FINISH,
     * TopologyState.TOPOLOGY_STATE_FINISHED)
     * .and().addTransition(TopologyTransition.TOPOLOGY_TRANSITION_CANCEL,
     * TopologyState.TOPOLOGY_STATE_CANCELED)
     * .and().addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FAIL,
     * TopologyState.TOPOLOGY_STATE_FAILURE) .defineState(TopologyState.TOPOLOGY_STATE_FINISHED)
     * .noTransition() .defineState(TopologyState.TOPOLOGY_STATE_CANCELED) .noTransition()
     * .defineState(TopologyState.TOPOLOGY_STATE_FAILURE) .noTransition()
     * .defineState(TopologyState.ERROR) .noTransition()
     * .setInitialState(TopologyState.TOPOLOGY_STATE_CREATED) .build();
     * 
     * // ---------------------------------------------------
     * 
     * topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_RUNNING, new
     * FSMStateAction<TopologyState, TopologyTransition>() {
     * 
     * @Override public void stateAction(TopologyState previousState, TopologyTransition transition,
     * TopologyState state) { System.out.println("RUNNING STATE LISTENER"); } });
     * 
     * topologyFSM.addStateListener(TopologyState.ERROR, new FSMStateAction<TopologyState,
     * TopologyTransition>() {
     * 
     * @Override public void stateAction(TopologyState previousState, TopologyTransition transition,
     * TopologyState state) { System.out.println("ERROR STATE LISTENER"); } });
     * 
     * topologyFSM.addGlobalStateListener(new FSMStateAction<TopologyState, TopologyTransition>() {
     * 
     * @Override public void stateAction(TopologyState previousState, TopologyTransition transition,
     * TopologyState state) { System.out.println("GLOBAL LISTENER"); } });
     * 
     * topologyFSM.start();
     * 
     * topologyFSM.dispatchEvent(new
     * FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE));
     * 
     * topologyFSM.dispatchEvent(new
     * FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));
     * 
     * topologyFSM.dispatchEvent(new
     * FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY));
     * 
     * 
     * //topologyFSM.dispatchEvent(new
     * FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_RUN));
     * 
     * topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
     * 
     * topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
     * 
     * topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
     * 
     * topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
     * 
     * topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));
     * 
     * 
     * topologyFSM.dispatchEvent(new
     * FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_FINISH)); }
     */
}
