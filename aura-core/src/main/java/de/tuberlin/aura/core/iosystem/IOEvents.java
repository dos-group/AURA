package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.iosystem.RPCManager.MethodSignature;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import io.netty.channel.Channel;

import java.util.UUID;

public final class IOEvents {

    // Disallow instantiation.
    private IOEvents() {
    }

    /**
     *
     */
    public static final class DataEventType {

        private DataEventType() {
        }

        public static final String DATA_EVENT_INPUT_CHANNEL_CONNECTED = "DATA_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_CHANNEL_CONNECTED = "DATA_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_GATE_OPEN = "DATA_EVENT_OUTPUT_GATE_OPEN";

        public static final String DATA_EVENT_OUTPUT_GATE_CLOSE = "DATA_EVENT_OUTPUT_GATE_CLOSE";

        public static final String DATA_EVENT_BUFFER = "DATA_EVENT_BUFFER";

        public static final String DATA_EVENT_SOURCE_EXHAUSTED = "DATA_EVENT_SOURCE_EXHAUSTED";
    }

    /**
     *
     */
    public static final class ControlEventType {

        private ControlEventType() {
        }

        public static final String CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_INPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_MESSAGE = "CONTROL_EVENT_MESSAGE";

        public static final String CONTROL_EVENT_RPC_CALLER_REQUEST = "CONTROL_EVENT_RPC_CALLER_REQUEST";

        public static final String CONTROL_EVENT_RPC_CALLEE_RESPONSE = "CONTROL_EVENT_RPC_CALLEE_RESPONSE";

        public static final String CONTROL_EVENT_TASK_STATE = "CONTROL_EVENT_TASK_STATE";

        public static final String CONTROL_EVENT_INCOMPLETE_EVENT = "CONTROL_EVENT_INCOMPLETE_EVENT";
    }

    /**
     *
     */
    public static class BaseIOEvent extends Event {

        private static final long serialVersionUID = 1L;

        public BaseIOEvent(final String type) {
            super(type);
        }

        private Channel channel;

        public void setChannel(final Channel channel) {
            // sanity check.
            if (channel == null)
                throw new IllegalArgumentException("channel == null");

            this.channel = channel;
        }

        public Channel getChannel() {
            return channel;
        }
    }

    public static class GenericIOEvent extends DataIOEvent {
        public final Object payload;

        public GenericIOEvent(final String type, final Object payload, final UUID srcTaskID, final UUID dstTaskID) {
            super(type, srcTaskID, dstTaskID);

            this.payload = payload;
        }

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append("GenericIOEvent = {")
                    .append(" type = " + type + ", ")
                    .append(" payload = " + payload.toString() + ", ")
                    .append(" }").toString();
        }
    }

    /**
     *
     */
    public static class DataIOEvent extends BaseIOEvent {

        private static final long serialVersionUID = 1L;

        public DataIOEvent(final String type,
                           final UUID srcTaskID,
                           final UUID dstTaskID) {
            super(type);
            // sanity check.
            if (srcTaskID == null)
                throw new IllegalArgumentException("srcTaskID == null");
            if (dstTaskID == null)
                throw new IllegalArgumentException("dstTaskID == null");

            this.srcTaskID = srcTaskID;

            this.dstTaskID = dstTaskID;
        }

        public final UUID srcTaskID;

        public final UUID dstTaskID;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append("DataIOEvent = {")
                    .append(" type = " + type + ", ")
                    .append(" srcTaskID = " + srcTaskID.toString() + ", ")
                    .append(" dstTaskID = " + dstTaskID.toString())
                    .append(" }").toString();
        }
    }

    /**
     *
     */
    public static final class DataBufferEvent extends DataIOEvent {

        private static final long serialVersionUID = -1;

        public DataBufferEvent(final UUID srcTaskID,
                               final UUID dstTaskID,
                               final byte[] data) {
            this(UUID.randomUUID(), srcTaskID, dstTaskID, data);
        }

        public DataBufferEvent(final UUID messageID,
                               final UUID srcTaskID,
                               final UUID dstTaskID,
                               final byte[] data) {

            super(DataEventType.DATA_EVENT_BUFFER, srcTaskID, dstTaskID);

            // sanity check.
            if (messageID == null)
                throw new IllegalArgumentException("messageID == null");
            if (data == null)
                throw new IllegalArgumentException("data == null");

            this.messageID = messageID;

            this.data = data;
        }

        public final UUID messageID;

        public final byte[] data;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append("DataBufferMessage = {")
                    .append(" messageID = " + messageID.toString() + ", ")
                    .append(" srcTaskID = " + srcTaskID.toString() + ", ")
                    .append(" dstTaskID = " + dstTaskID.toString() + ", ")
                    .append(" data = " + data + ", ")
                    .append(" length( data ) = " + data.length)
                    .append(" }").toString();
        }
    }

    /**
     *
     */
    public static class ControlIOEvent extends BaseIOEvent {

        private static final long serialVersionUID = 1L;

        public ControlIOEvent(final String type) {
            super(type);
        }

        public ControlIOEvent(final String type,
                              final UUID srcMachineID,
                              final UUID dstMachineID) {
            super(type);
            // sanity check.
            if (srcMachineID == null)
                throw new IllegalArgumentException("srcMachineID == null");
            if (dstMachineID == null)
                throw new IllegalArgumentException("dstMachineID == null");

            this.srcMachineID = srcMachineID;

            this.dstMachineID = dstMachineID;
        }

        private UUID srcMachineID;

        private UUID dstMachineID;

        public void setSrcMachineID(final UUID srcMachineID) {
            // sanity check.
            if (srcMachineID == null)
                throw new IllegalArgumentException("srcMachineID == null");

            this.srcMachineID = srcMachineID;
        }

        public UUID getSrcMachineID() {
            return this.srcMachineID;
        }

        public void setDstMachineID(final UUID dstMachineID) {
            // sanity check.
            if (dstMachineID == null)
                throw new IllegalArgumentException("dstMachineID == null");

            this.dstMachineID = dstMachineID;
        }

        public UUID getDstMachineID() {
            return this.dstMachineID;
        }

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append("ControlIOEvent = {")
                    .append(" type = " + type + ", ")
                    .append(" srcMachineID = " + srcMachineID.toString() + ", ")
                    .append(" dstMachineID = " + dstMachineID.toString())
                    .append(" }").toString();
        }
    }

    /**
     *
     */
    public static final class RPCCallerRequestEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public RPCCallerRequestEvent(final UUID callUID,
                                     final MethodSignature methodSignature) {

            super(ControlEventType.CONTROL_EVENT_RPC_CALLER_REQUEST);
            // sanity check.
            if (callUID == null)
                throw new IllegalArgumentException("callUID == null");
            if (methodSignature == null)
                throw new IllegalArgumentException("methodSignature == null");

            this.callUID = callUID;

            this.methodSignature = methodSignature;
        }

        public UUID callUID;

        public MethodSignature methodSignature;
    }

    /**
     *
     */
    public static final class RPCCalleeResponseEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public RPCCalleeResponseEvent(final UUID callUID,
                                      final Object result) {

            super(ControlEventType.CONTROL_EVENT_RPC_CALLEE_RESPONSE);
            // sanity check.
            if (callUID == null)
                throw new IllegalArgumentException("callUID == null");

            this.callUID = callUID;

            this.result = result;
        }

        public UUID callUID;

        public Object result;
    }

    /**
     *
     */
    public static final class TaskStateEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public TaskStateEvent(final UUID topologyID, final UUID taskID, final TaskState state) {
            super(ControlEventType.CONTROL_EVENT_TASK_STATE);

            this.topologyID = topologyID;

            this.taskID = taskID;

            this.state = state;
        }

        public final UUID topologyID;

        public final UUID taskID;

        public final TaskState state;
    }

    /**
     *
     */
    public static final class TaskStateTransitionEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public static final String TASK_STATE_TRANSITION_EVENT = "TASK_STATE_TRANSITION_EVENT";

        public TaskStateTransitionEvent(final TaskTransition transition) {
            this(null, transition);
        }

        public TaskStateTransitionEvent(final UUID topologyID, final TaskTransition transition) {
            super(TASK_STATE_TRANSITION_EVENT);
            // sanity check.
            if (transition == null)
                throw new IllegalArgumentException("transition == null");

            this.topologyID = topologyID;

            this.transition = transition;
        }

        public final TaskTransition transition;

        public final UUID topologyID;

        @Override
        public String toString() {
            return (new StringBuilder())
                    .append("TaskChangeStateEvent = {")
                    .append(" type = " + super.type + ", ")
                    .append(" taskID = " + topologyID + ", ")
                    .append(" transition = " + transition.toString())
                    .append(" }").toString();
        }
    }
}
