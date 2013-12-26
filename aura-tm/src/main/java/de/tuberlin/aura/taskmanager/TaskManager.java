package de.tuberlin.aura.taskmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskStateMachine;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.task.usercode.UserCodeImplanter;

public final class TaskManager implements WM2TMProtocol {

    //---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

    /**
     *
     */
    private final class IORedispatcher extends EventHandler {

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED )
        private void handleDataOutputChannelEvent( final DataIOEvent event ) {
            final Pair<TaskContext,IEventDispatcher> contextAndHandler = taskContextMap.get( event.srcTaskID );
            contextAndHandler.getSecond().dispatchEvent( event );
        }

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED )
        private void handleDataInputChannelEvent( final DataIOEvent event ) {
            final Pair<TaskContext,IEventDispatcher> contextAndHandler = taskContextMap.get( event.dstTaskID );
            contextAndHandler.getSecond().dispatchEvent( event );
        }

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN )
        private void handleDataChannelGateOpenEvent( final DataIOEvent event ) {
            final Pair<TaskContext,IEventDispatcher> contextAndHandler = taskContextMap.get( event.srcTaskID );
            contextAndHandler.getSecond().dispatchEvent( event );
        }

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE )
        private void handleDataChannelGateCloseEvent( final DataIOEvent event ) {
            final Pair<TaskContext,IEventDispatcher> contextAndHandler = taskContextMap.get( event.srcTaskID );
            contextAndHandler.getSecond().dispatchEvent( event );
        }

        @Handle( event = DataBufferEvent.class )
        private void handleDataEvent( final DataBufferEvent event ) {
            final Pair<TaskContext,IEventDispatcher> contextAndHandler = taskContextMap.get( event.dstTaskID );
            contextAndHandler.getSecond().dispatchEvent( event );
        }

        @Handle( event = TaskStateTransitionEvent.class )
        private void handleTaskStateTransitionEvent( final TaskStateTransitionEvent event ) {
            final List<TaskContext> contextList = topologyTaskContextMap.get( event.topologyID );
            if( contextList == null )
                throw new IllegalArgumentException( "contextList == null" );
            for( final TaskContext tc : contextList )
                tc.dispatcher.dispatchEvent( event );
        }
    }

    /**
     *
     */
    private final class TaskEventHandler extends EventHandler {

        public TaskContext context;

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED )
        private void handleTaskInputDataChannelConnect( final DataIOEvent event ) {
            int gateIndex = 0;
            boolean allInputGatesConnected = true, connectingToCorrectTask = false;
            for( final List<TaskDescriptor> inputGate : context.taskBinding.inputGateBindings ) {
                int channelIndex = 0;
                boolean allInputChannelsPerGateConnected = true;
                for( TaskDescriptor inputTask : inputGate ) {
                    // Set the channel on right position.
                    if( inputTask.taskID.equals( event.srcTaskID ) ) {
                        context.inputGates.get( gateIndex ).setChannel( channelIndex, event.getChannel() );
                        LOG.info( "INPUT CONNECTION FROM " + inputTask.name + " [" + inputTask.taskID + "] TO TASK "
                                + context.task.name + " [" + context.task.taskID + "] IS ESTABLISHED" );
                        connectingToCorrectTask |= true;
                    }
                    // all data inputs are connected...
                    allInputChannelsPerGateConnected &= ( context.inputGates.get( gateIndex ).getChannel( channelIndex++ ) != null );
                }

                allInputGatesConnected &= allInputChannelsPerGateConnected;
                ++gateIndex;
            }

            // Check if the incoming channel is connecting to the correct task.
            if( !connectingToCorrectTask )
                throw new IllegalStateException( "wrong data channel tries to connect" );

            if( allInputGatesConnected ) {
                context.dispatcher.dispatchEvent( new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED ) );
            }
        }

        @Handle( event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED )
        private void handleTaskOutputDataChannelConnect( final DataIOEvent event ) {
            int gateIndex = 0;
            boolean allOutputGatesConnected = true;
            for( final List<TaskDescriptor> outputGate : context.taskBinding.outputGateBindings ) {
                int channelIndex = 0;
                boolean allOutputChannelsPerGateConnected = true;
                for( TaskDescriptor outputTask : outputGate ) {
                    // Set the channel on right position.
                    if( outputTask.taskID.equals( event.dstTaskID ) ) {
                        context.outputGates.get( gateIndex ).setChannel( channelIndex, event.getChannel() );
                        LOG.info( "OUTPUT CONNECTION FROM " + context.task.name + " [" + context.task.taskID + "] TO TASK "
                                + outputTask.name + " [" + outputTask.taskID + "] IS ESTABLISHED" );
                    }
                    // all data outputs are connected...
                    allOutputChannelsPerGateConnected &= ( context.outputGates.get( gateIndex ).getChannel( channelIndex++ ) != null );
                }
                allOutputGatesConnected &= allOutputChannelsPerGateConnected;
                ++gateIndex;
            }

            if( allOutputGatesConnected ) {
                context.dispatcher.dispatchEvent( new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED ) );
            }
        }

        @Handle( event = DataBufferEvent.class )
        private void handleTaskInputData( final DataBufferEvent event ) {
            context.inputGates.get( context.getInputChannelIndexFromTaskID( event.srcTaskID ) )
                .addToInputQueue( event );
        }

        @Handle( event = TaskStateTransitionEvent.class )
        private void handleTaskStateTransition( final TaskStateTransitionEvent event ) {
            synchronized( context.state ) { // serialize task state transitions!
                final TaskState oldState = context.state;
                final Map<TaskTransition,TaskState> transitionsSpace =
                        TaskStateMachine.TASK_STATE_TRANSITION_MATRIX.get( context.state );
                final TaskState nextState = transitionsSpace.get( event.transition );
                context.state = nextState;
                // Trigger state dependent actions. Realization of a classic Moore automata.
                switch( context.state ) {
                    case TASK_STATE_NOT_CONNECTED: {} break;
                    case TASK_STATE_INPUTS_CONNECTED: {} break;
                    case TASK_STATE_OUTPUTS_CONNECTED: {} break;
                    case TASK_STATE_READY: {
                        ioManager.sendEvent( wmMachine, new TaskStateEvent( context.task.topologyID, context.task.taskID, context.state ) );
                    } break;
                    case TASK_STATE_RUNNING: {
                        scheduleTask( context );
                    } break;
                    case TASK_STATE_FINISHED: {
                        ioManager.sendEvent( wmMachine, new TaskStateEvent( context.task.topologyID, context.task.taskID, context.state ) );
                        taskContextMap.remove( context.task.taskID );
                        topologyTaskContextMap.get( context.task.topologyID ).remove( context );
                        context.close();
                    } break;
                    case TASK_STATE_FAILURE: {} break;
                    case TASK_STATE_RECOVER: {} break;
                    case TASK_STATE_UNDEFINED: {
                        throw new IllegalStateException( "task " + context.task.name + " [" + context.task.taskID + "] from state "
                                + oldState + " to " + context.state + " is not defined" );
                    }
                }
                LOG.info( "CHANGE STATE OF TASK " + context.task.name + " [" + context.task.taskID + "] FROM "
                        + oldState + " TO " + context.state );
            }
        }
    }

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TaskManager( final MachineDescriptor machine, final MachineDescriptor workloadManagerMachine ) {
        // sanity check.
        if( machine == null )
            throw new IllegalArgumentException( "machine == null" );

        this.wmMachine = workloadManagerMachine;
        this.taskContextMap = new ConcurrentHashMap<UUID, Pair<TaskContext,IEventDispatcher>>();
        this.topologyTaskContextMap = new ConcurrentHashMap<UUID, List<TaskContext>>();
        this.ioManager = new IOManager( machine );
        this.rpcManager = new RPCManager( ioManager );
        this.ioHandler = new IORedispatcher();
        this.codeImplanter = new UserCodeImplanter( this.getClass().getClassLoader() );

        final int N = 4;
        this.executionUnit = new TaskExecutionUnit[N];
        for( int i = 0; i < N; ++i ) {
            this.executionUnit[i] = new TaskExecutionUnit( i );
            this.executionUnit[i].start();
        }

        final String[] IOEvents =
            { DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
              DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
              DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN,
              DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE,
              DataEventType.DATA_EVENT_BUFFER,
              TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT };

        this.ioManager.addEventListener( IOEvents, ioHandler );

        rpcManager.registerRPCProtocolImpl( this, WM2TMProtocol.class );
        if( workloadManagerMachine != null)
            ioManager.connectMessageChannelBlocking( workloadManagerMachine );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( TaskManager.class );

    private final MachineDescriptor wmMachine;

    private final Map<UUID, Pair<TaskContext,IEventDispatcher>> taskContextMap;

    private final Map<UUID, List<TaskContext>> topologyTaskContextMap;

    private final IOManager ioManager;

    private final IORedispatcher ioHandler;

    private final RPCManager rpcManager;

    private final TaskExecutionUnit[] executionUnit;

    private final UserCodeImplanter codeImplanter;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    @Override
    public void installTask( final TaskDeploymentDescriptor taskDeploymentDescriptor ) {
        // sanity check.
        if( taskDeploymentDescriptor == null )
            throw new IllegalArgumentException( "taskDescriptor == null" );

        @SuppressWarnings("unchecked")
        final Class<? extends TaskInvokeable> userCodeClass =
            (Class<? extends TaskInvokeable>) codeImplanter.implantUserCodeClass( taskDeploymentDescriptor.taskDescriptor.userCode );

        installTask( taskDeploymentDescriptor.taskDescriptor,
                     taskDeploymentDescriptor.taskBindingDescriptor,
                     userCodeClass );
    }

    // TODO: Make that later private!
    public void installTask( final TaskDescriptor taskDescriptor,
                             final TaskBindingDescriptor taskBindingDescriptor,
                             final Class<? extends TaskInvokeable> executableClass ) {

        final TaskEventHandler handler = new TaskEventHandler();
        final TaskContext context = new TaskContext( taskDescriptor, taskBindingDescriptor, handler, executableClass );
        handler.context = context;
        taskContextMap.put( taskDescriptor.taskID, new Pair<TaskContext,IEventDispatcher>( context, context.dispatcher ) );

        if( taskBindingDescriptor.inputGateBindings.size() == 0 ) {
            context.dispatcher.dispatchEvent( new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_INPUTS_CONNECTED ) );
        }

        if( taskBindingDescriptor.outputGateBindings.size() == 0 ) {
            context.dispatcher.dispatchEvent( new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_OUTPUTS_CONNECTED ) );
        }

        // TODO: To allow cycles in the execution graph we have to split up
        // installation and wiring of tasks in the deployment phase!
        wireOutputDataChannels( taskDescriptor, taskBindingDescriptor );

        List<TaskContext> contextList = topologyTaskContextMap.get( taskDescriptor.topologyID );
        if( contextList == null ) {
            contextList = new ArrayList<TaskContext>();
            topologyTaskContextMap.put( taskDescriptor.topologyID, contextList );
        }
        contextList.add( context );
    }

    public RPCManager getRPCManager() {
        return rpcManager;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    //---------------------------------------------------
    // Private.
    //---------------------------------------------------

    private synchronized void wireOutputDataChannels( final TaskDescriptor taskDescriptor,
            final TaskBindingDescriptor taskBindingDescriptor ) {
        // Connect outputs, if we have some...
        if( taskBindingDescriptor.outputGateBindings.size() > 0 ) {
            for( final List<TaskDescriptor> outputGate : taskBindingDescriptor.outputGateBindings )
                for( final TaskDescriptor outputTask : outputGate )
                    ioManager.connectDataChannel( taskDescriptor.taskID, outputTask.taskID, outputTask.getMachineDescriptor() );
        }
    }

    private void scheduleTask( final TaskContext context ) {
        // sanity check.
        if( context == null )
            throw new IllegalArgumentException( "context must not be null" );
        final int N = 4;
        int tmpMin, tmpMinOld;
        tmpMin = tmpMinOld = executionUnit[0].getNumberOfEnqueuedTasks();
        int selectedEU = 0;
        for( int i = 1; i < N; ++i ) {
            tmpMin = executionUnit[i].getNumberOfEnqueuedTasks();
            if( tmpMin < tmpMinOld ) {
                tmpMinOld = tmpMin;
                selectedEU = i;
            }
        }
        executionUnit[selectedEU].enqueueTask( context );
        LOG.info( "EXECUTE TASK " + context.task.name + " [" + context.task.taskID + "]"
                + " ON EXECUTIONUNIT (" + executionUnit[selectedEU].getExecutionUnitID() + ")" );
    }
}
