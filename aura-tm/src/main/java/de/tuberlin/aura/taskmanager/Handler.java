package de.tuberlin.aura.taskmanager;

import io.netty.channel.Channel;

import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.taskmanager.TaskEvents.TaskStateTransitionEvent;

public final class Handler {

    // Disallow instantiation.
    private Handler() {}

    /**
     *
     */
    public static abstract class AbstractTaskEventHandler implements IEventHandler {

        protected TaskContext context;

        public void setContext( final TaskContext context ) {
            // sanity check.
            if( context == null )
                throw new IllegalArgumentException( "context == null" );

            this.context = context;
        }

        @Override
        public void handleEvent( final Event e ) {
            if( e instanceof IOEvents.IODataChannelEvent ) {
                final IOEvents.IODataChannelEvent event = (IOEvents.IODataChannelEvent)e;

                if( IOEvents.IODataChannelEvent.IO_EVENT_OUTPUT_CHANNEL_CONNECTED.equals( e.type ) )
                    handleTaskOutputDataChannelConnect( event.srcTaskID, event.dstTaskID, event.channel );

                if( IOEvents.IODataChannelEvent.IO_EVENT_INPUT_CHANNEL_CONNECTED.equals( e.type ) )
                    handleTaskInputDataChannelConnect( event.srcTaskID, event.dstTaskID, event.channel );

            } else if( e instanceof TaskStateTransitionEvent ) {
                final TaskStateTransitionEvent event = (TaskStateTransitionEvent) e;

                if( TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT.equals( event.type ) )
                    handleTaskStateTransition( context.state, event.transition );

            } else if( e instanceof IOEvents.IODataEvent ) {
                final IOEvents.IODataEvent event = (IOEvents.IODataEvent) e;

                handleInputData( event.message );

            } else {
                throw new IllegalStateException( "Can not handle this event type." );
            }
        }

        protected abstract void handleTaskInputDataChannelConnect( UUID srcTaskID, UUID dstTaskID, Channel channel );

        protected abstract void handleTaskOutputDataChannelConnect( UUID srcTaskID, UUID dstTaskID, Channel channel );

        protected abstract void handleInputData( DataMessage message );

        protected abstract void handleTaskStateTransition( TaskState currentState, TaskTransition transition );

        protected abstract void handleTaskException();
    }
}
