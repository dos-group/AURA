package de.tuberlin.aura.core.common.statemachine;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.utils.Pair;

import java.util.*;

public final class StateMachine {

    // Disallow instantiation.
    private StateMachine() {
    }

    public static final class FiniteStateMachineBuilder<S extends Enum<S>, T extends Enum<T>> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private S initialState;

        private final S errorState;

        private final Class<T> transitionClazz;

        private final Map<S, Pair<StateAction<S, T>, Map<T, S>>> stateTransitionMtx;

        private final TransitionBuilder<S, T> transitionBuilder;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

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
        }

        // ---------------------------------------------------
        // Public.
        // ---------------------------------------------------

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

        public TransitionBuilder<S, T> defineState(final S state, final StateAction<S, T> action) {
            // sanity check.
            if (state == null)
                throw new IllegalArgumentException("state == null");

            if (stateTransitionMtx.get(state) != null)
                throw new IllegalStateException("state already defined");

            final Map<T, S> transitionMap = new HashMap<>();
            for (final T transition : transitionClazz.getEnumConstants()) {
                transitionMap.put(transition, errorState);
            }

            stateTransitionMtx.put(state, new Pair<>(action, transitionMap));

            return transitionBuilder.currentState(state);
        }

        public TransitionBuilder<S, T> defineState(final S state) {
            return defineState(state, null);
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

            return new FiniteStateMachine<>(
                    Collections.unmodifiableMap(stateTransitionMtx),
                    initialState,
                    errorState,
                    Collections.unmodifiableSet(finalStates),
                    Collections.unmodifiableMap(nestedFSMs),
                    transitionClazz);
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
            // Public.
            // ---------------------------------------------------

            public FiniteStateMachineBuilder<S, T> addTransition(final T transition, final S nextState) {
                // sanity check.
                if (transition == null)
                    throw new IllegalArgumentException("transition == null");
                if (nextState == null)
                    throw new IllegalArgumentException("nextState == null");

                final Map<T, S> transitionMap = fsmBuilder.stateTransitionMtx.get(currentState).getSecond();

                if (transitionMap.get(transition) != errorState) {
                    throw new IllegalStateException("transition already defined");
                }

                transitionMap.put(transition, nextState);
                return fsmBuilder;
            }

            public FiniteStateMachineBuilder<S, T> noTransition() {
                // sanity check.
                if (fsmBuilder.finalStates.contains(currentState))
                    throw new IllegalStateException();
                fsmBuilder.finalStates.add(currentState);
                return fsmBuilder;
            }

            // ---------------------------------------------------
            // Private.
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
    public static final class FiniteStateMachine<S, T> extends EventDispatcher {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private S currentState;

        private final Map<S, Pair<StateAction<S, T>, Map<T, S>>> stateTransitionMtx;

        private final S errorState;

        private final Set<S> finalStates;

        private final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs;

        private final Class<T> transitionClazz;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public FiniteStateMachine(final Map<S, Pair<StateAction<S, T>, Map<T, S>>> stateTransitionMtx,
                                  final S initialState,
                                  final S errorState,
                                  final Set<S> finalStates,
                                  final Map<S, List<FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>>>> nestedFSMs,
                                  final Class<T> transitionClazz) {
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

            this.stateTransitionMtx = stateTransitionMtx;

            this.currentState = initialState;

            this.errorState = errorState;

            this.finalStates = finalStates;

            this.nestedFSMs = nestedFSMs;

            this.transitionClazz = transitionClazz;

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

            for (final T transition : transitionClazz.getEnumConstants()) {
                if (transition instanceof TransitionAction) {
                    ((TransitionAction) transition).setUp(this);
                }
            }
        }

        // ---------------------------------------------------
        // Public.
        // ---------------------------------------------------

        public void start() {
            stateTransitionMtx.get(currentState).getFirst().stateAction(null, null, currentState);
        }

        public void doTransition(final Object transitionObj) {
            // sanity check.
            if (transitionObj == null)
                throw new IllegalArgumentException("transition == null ");
            if (transitionObj.getClass() != transitionClazz)
                throw new IllegalStateException();

            @SuppressWarnings("unchecked")
            final T transition = (T) transitionObj;

            if (finalStates.contains(currentState)) {
                throw new IllegalStateException(currentState + " is a final state, no transition allowed");
            }

            final Pair<StateAction<S, T>, Map<T, S>> transitionSpace = stateTransitionMtx.get(currentState);
            final S nextState = transitionSpace.getSecond().get(transition);

            if (nextState == errorState) {
                transitionSpace.getFirst().stateAction(currentState, transition, nextState);
            } else {
                stateTransitionMtx.get(nextState).getFirst().stateAction(currentState, transition, nextState);

                for (final FiniteStateMachine<? extends Enum<?>, ? extends Enum<?>> nestedFSM : nestedFSMs.get(nextState)) {
                    nestedFSM.start();
                }
            }

            currentState = nextState;
        }

        public Class<T> getTransitionClazz() {

            return transitionClazz;
        }

        public S getCurrentState() {
            return currentState;
        }
    }


    public static interface StateAction<S, T> {

        public void stateAction(final S previousState, final T transition, final S state);
    }


    public static interface TransitionAction<S, T> {

        public void setUp(final FiniteStateMachine<S, T> stateMachine);
    }


    public static class ConsensusTransitionAction<S, T extends Enum<T>, S1, T1 extends Enum<T1>> implements TransitionAction<S, T> {

        private int count;

        private final T1 localTransition;

        private final T globalTransition;

        public ConsensusTransitionAction(final int count, final T1 localTransition, final T globalTransition) {
            this.count = count;
            this.localTransition = localTransition;
            this.globalTransition = globalTransition;
        }

        @Override
        public void setUp(final FiniteStateMachine<S, T> stateMachine) {
            stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + localTransition.toString(), new IEventHandler() {

                @Override
                public void handleEvent(Event event) {
                    if ((--count) == 0) {
                        stateMachine.dispatchEvent(new FSMTransitionEvent<T>(globalTransition));
                    }
                }
            });
        }
    }


    public static class FSMTransitionEvent<T extends Enum<T>> extends Event {

        public static final String FSMTransition = "FSM_TRANSITION_";

        public FSMTransitionEvent(final T transition) {
            super(FSMTransition + transition.toString(), transition);
        }
    }

    // ---------------------------------------------------
    // Topology State Machine.
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
    public static enum TopologyTransition implements TransitionAction<TopologyState, TopologyTransition> {

        TOPOLOGY_TRANSITION_PARALLELIZE,

        TOPOLOGY_TRANSITION_SCHEDULE,

        TOPOLOGY_TRANSITION_DEPLOY,

        TOPOLOGY_TRANSITION_RUN(new TransitionAction<TopologyState, TopologyTransition>() {

            private int counter = 5;

            @Override
            public void setUp(final FiniteStateMachine<TopologyState, TopologyTransition> stateMachine) {

                stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + TaskTransition.TASK_TRANSITION_RUN.toString(), new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {

                        if (stateMachine.getCurrentState() == TopologyState.TOPOLOGY_STATE_DEPLOYED) {
                            if ((--counter) == 0) {
                                stateMachine.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_RUN));
                            }
                        }
                    }
                });
            }
        }),

        TOPOLOGY_TRANSITION_FINISH(new TransitionAction<TopologyState, TopologyTransition>() {

            private int counter = 5;

            @Override
            public void setUp(final FiniteStateMachine<TopologyState, TopologyTransition> stateMachine) {

                stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + TaskTransition.TASK_TRANSITION_FINISH.toString(), new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {

                        if (stateMachine.getCurrentState() == TopologyState.TOPOLOGY_STATE_RUNNING) {
                            if ((--counter) == 0) {
                                stateMachine.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_FINISH));
                            }
                        }
                    }
                });
            }
        }),

        TOPOLOGY_TRANSITION_CANCEL(new TransitionAction<TopologyState, TopologyTransition>() {

            private int counter = 5;

            @Override
            public void setUp(final FiniteStateMachine<TopologyState, TopologyTransition> stateMachine) {

                stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + TaskTransition.TASK_TRANSITION_CANCEL.toString(), new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {

                        if (stateMachine.getCurrentState() == TopologyState.TOPOLOGY_STATE_RUNNING) {
                            if ((--counter) == 0) {
                                stateMachine.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_CANCEL));
                            }
                        }
                    }
                });
            }
        }),

        TOPOLOGY_TRANSITION_FAIL(new TransitionAction<TopologyState, TopologyTransition>() {

            @Override
            public void setUp(final FiniteStateMachine<TopologyState, TopologyTransition> stateMachine) {

                stateMachine.addEventListener(FSMTransitionEvent.FSMTransition + TaskTransition.TASK_TRANSITION_FAIL.toString(), new IEventHandler() {
                    @Override
                    public void handleEvent(Event event) {

                        if (stateMachine.getCurrentState() == TopologyState.TOPOLOGY_STATE_RUNNING) {
                            stateMachine.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_FAIL));
                        }
                    }
                });
            }
        });

        public final TransitionAction<TopologyState, TopologyTransition> action;

        TopologyTransition() {
            this(null);
        }

        TopologyTransition(final TransitionAction<TopologyState, TopologyTransition> action) {
            this.action = action;
        }

        @Override
        public void setUp(final FiniteStateMachine<TopologyState, TopologyTransition> stateMachine) {
            if (action != null)
                action.setUp(stateMachine);
        }
    }

    // ---------------------------------------------------
    // Task State Machine.
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

    // ---------------------------------------------------
    // Operator State Machine.
    // ---------------------------------------------------

    /**
     *
     */
    public static enum OperatorState {

        OPERATOR_STATE_CREATED,

        OPERATOR_STATE_OPENED,

        OPERATOR_STATE_RUNNING,

        OPERATOR_STATE_CLOSED,

        OPERATOR_STATE_RELEASED,

        ERROR
    }

    /**
     *
     */
    public static enum OperatorTransition {

        OPERATOR_TRANSITION_OPEN,

        OPERATOR_TRANSITION_RUN,

        OPERATOR_TRANSITION_CLOSE,

        OPERATOR_TRANSITION_RESET,

        OPERATOR_TRANSITION_RELEASE
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        // ---------------------------------------------------

        final StateAction<OperatorState, OperatorTransition> operatorAction = new StateAction<OperatorState, OperatorTransition>() {
            @Override
            public void stateAction(OperatorState previousState, OperatorTransition transition, OperatorState state) {
                System.out.println("previousState = " + previousState
                        + " - transition = " + transition
                        + " - state = " + state);
            }
        };

        final FiniteStateMachineBuilder<OperatorState, OperatorTransition> operatorFSMBuilder
                = new FiniteStateMachineBuilder<>(OperatorState.class, OperatorTransition.class, OperatorState.ERROR);

        final FiniteStateMachine<OperatorState, OperatorTransition> operatorFSM = operatorFSMBuilder
                .defineState(OperatorState.OPERATOR_STATE_CREATED, operatorAction)
                .addTransition(OperatorTransition.OPERATOR_TRANSITION_OPEN, OperatorState.OPERATOR_STATE_OPENED)
                .defineState(OperatorState.OPERATOR_STATE_OPENED, operatorAction)
                .addTransition(OperatorTransition.OPERATOR_TRANSITION_RUN, OperatorState.OPERATOR_STATE_RUNNING)
                .defineState(OperatorState.OPERATOR_STATE_RUNNING, operatorAction)
                .addTransition(OperatorTransition.OPERATOR_TRANSITION_CLOSE, OperatorState.OPERATOR_STATE_CLOSED)
                .defineState(OperatorState.OPERATOR_STATE_CLOSED, operatorAction)
                .addTransition(OperatorTransition.OPERATOR_TRANSITION_RELEASE, OperatorState.OPERATOR_STATE_RELEASED)
                .and().addTransition(OperatorTransition.OPERATOR_TRANSITION_RESET, OperatorState.OPERATOR_STATE_OPENED)
                .defineState(OperatorState.OPERATOR_STATE_RELEASED, operatorAction)
                .noTransition()
                .setInitialState(OperatorState.OPERATOR_STATE_CREATED)
                .build();

        // ---------------------------------------------------

        final StateAction<TaskState, TaskTransition> taskAction = new StateAction<TaskState, TaskTransition>() {
            @Override
            public void stateAction(TaskState previousState, TaskTransition transition, TaskState state) {
                System.out.println("previousState = " + previousState
                        + " - transition = " + transition
                        + " - state = " + state);
            }
        };
        final FiniteStateMachineBuilder<TaskState, TaskTransition> taskFSMBuilder
                = new FiniteStateMachineBuilder<>(TaskState.class, TaskTransition.class, TaskState.ERROR);

        taskFSMBuilder
                .defineState(TaskState.TASK_STATE_CREATED, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_INPUTS_CONNECTED)
                .and().addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_OUTPUTS_CONNECTED)
                .defineState(TaskState.TASK_STATE_INPUTS_CONNECTED, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                .defineState(TaskState.TASK_STATE_OUTPUTS_CONNECTED, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED, TaskState.TASK_STATE_READY)
                .defineState(TaskState.TASK_STATE_READY, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_RUN, TaskState.TASK_STATE_RUNNING)
                .defineState(TaskState.TASK_STATE_RUNNING, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_FINISH, TaskState.TASK_STATE_FINISHED)
                .and().addTransition(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_CANCELED)
                .and().addTransition(TaskTransition.TASK_TRANSITION_FAIL, TaskState.TASK_STATE_FAILURE)
                .and().addTransition(TaskTransition.TASK_TRANSITION_SUSPEND, TaskState.TASK_STATE_PAUSED)
                .nestFSM(TaskState.TASK_STATE_RUNNING, operatorFSM)
                .defineState(TaskState.TASK_STATE_FINISHED, taskAction)
                .noTransition()
                .defineState(TaskState.TASK_STATE_CANCELED, taskAction)
                .noTransition()
                .defineState(TaskState.TASK_STATE_FAILURE, taskAction)
                .noTransition()
                .defineState(TaskState.TASK_STATE_PAUSED, taskAction)
                .addTransition(TaskTransition.TASK_TRANSITION_RESUME, TaskState.TASK_STATE_RUNNING)
                .setInitialState(TaskState.TASK_STATE_CREATED);

        // ---------------------------------------------------

        final StateAction<TopologyState, TopologyTransition> topologyAction = new StateAction<TopologyState, TopologyTransition>() {
            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                System.out.println("previousState = " + previousState
                        + " - transition = " + transition
                        + " - state = " + state);
            }
        };

        final FiniteStateMachineBuilder<TopologyState, TopologyTransition> topologyFSMBuilder
                = new FiniteStateMachineBuilder<>(TopologyState.class, TopologyTransition.class, TopologyState.ERROR);

        final FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM = topologyFSMBuilder
                .defineState(TopologyState.TOPOLOGY_STATE_CREATED, topologyAction)
                .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, TopologyState.TOPOLOGY_STATE_PARALLELIZED)
                .defineState(TopologyState.TOPOLOGY_STATE_PARALLELIZED, topologyAction)
                .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, TopologyState.TOPOLOGY_STATE_SCHEDULED)
                .defineState(TopologyState.TOPOLOGY_STATE_SCHEDULED, topologyAction)
                .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, TopologyState.TOPOLOGY_STATE_DEPLOYED)
                .defineState(TopologyState.TOPOLOGY_STATE_DEPLOYED, topologyAction)
                .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_RUN, TopologyState.TOPOLOGY_STATE_RUNNING)
                .defineState(TopologyState.TOPOLOGY_STATE_RUNNING, topologyAction)
                .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FINISH, TopologyState.TOPOLOGY_STATE_FINISHED)
                .and().addTransition(TopologyTransition.TOPOLOGY_TRANSITION_CANCEL, TopologyState.TOPOLOGY_STATE_CANCELED)
                .and().addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FAIL, TopologyState.TOPOLOGY_STATE_FAILURE)
                .defineState(TopologyState.TOPOLOGY_STATE_FINISHED, topologyAction)
                .noTransition()
                .defineState(TopologyState.TOPOLOGY_STATE_CANCELED, topologyAction)
                .noTransition()
                .defineState(TopologyState.TOPOLOGY_STATE_FAILURE, topologyAction)
                .noTransition()
                .defineState(TopologyState.ERROR)
                .noTransition()
                .setInitialState(TopologyState.TOPOLOGY_STATE_CREATED)
                .build();

        // ---------------------------------------------------

        topologyFSM.start();

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY));


        //topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_RUN));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_RUN));


        //topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_FINISH));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));

        topologyFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_FINISH));




        /*final List<FiniteStateMachine<TaskState, TaskTransition>> taskFSMList = new ArrayList<>();
        for(int i = 0; i < 15; ++i) {
            final FiniteStateMachine<TaskState, TaskTransition> taskFSM = taskFSMBuilder.build();
            taskFSMList.add(taskFSM);
            taskFSM.start();
            taskFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED));
            taskFSM.dispatchEvent(new FSMTransitionEvent<>(TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED));
        }*/
    }
}
