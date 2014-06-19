package de.tuberlin.aura.core.iosystem;

import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.iosystem.RPCManager.MethodSignature;
import de.tuberlin.aura.core.memory.MemoryView;
import io.netty.channel.Channel;

public final class IOEvents {

    // Disallow instantiation.
    private IOEvents() {}

    /**
     *
     */
    public static final class InternalEventType {

        private InternalEventType() {}

        public static final String POISON_PILL_TERMINATION = "POISON_PILL_TERMINATION";
    }

    /**
     *
     */
    public static final class DataEventType {

        private DataEventType() {}

        public static final String DATA_EVENT_INPUT_CHANNEL_CONNECTED = "DATA_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_CHANNEL_CONNECTED = "DATA_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_GATE_OPEN = "DATA_EVENT_OUTPUT_GATE_OPEN";

        public static final String DATA_EVENT_OUTPUT_GATE_CLOSE = "DATA_EVENT_OUTPUT_GATE_CLOSE";

        public static final String DATA_EVENT_OUTPUT_GATE_CLOSE_ACK = "DATA_EVENT_OUTPUT_GATE_CLOSE_ACK";

        public static final String DATA_EVENT_BUFFER = "DATA_EVENT_BUFFER";

        public static final String DATA_EVENT_SOURCE_EXHAUSTED = "DATA_EVENT_SOURCE_EXHAUSTED";

        public static final String DATA_EVENT_SOURCE_EXHAUSTED_ACK = "DATA_EVENT_SOURCE_EXHAUSTED_ACK";

        public static final String DATA_EVENT_RECORD_TYPE = "DATA_EVENT_RECORD_TYPE";
    }

    /**
     *
     */
    public static final class ControlEventType {

        private ControlEventType() {}

        public static final String CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_INPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_RPC_CALLER_REQUEST = "CONTROL_EVENT_RPC_CALLER_REQUEST";

        public static final String CONTROL_EVENT_RPC_CALLEE_RESPONSE = "CONTROL_EVENT_RPC_CALLEE_RESPONSE";

        public static final String CONTROL_EVENT_TOPOLOGY_FINISHED = "CONTROL_EVENT_TOPOLOGY_FINISHED";

        public static final String CONTROL_EVENT_TOPOLOGY_FAILURE = "CONTROL_EVENT_TOPOLOGY_FAILURE";

        public static final String CONTROL_EVENT_REMOTE_TASK_TRANSITION = "CONTROL_EVENT_REMOTE_TASK_TRANSITION";

        public static final String CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE = "CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE";
    }

    /**
     *
     */
    public static class BaseIOEvent extends Event {

        private static final long serialVersionUID = -1L;

        transient private Channel channel;

        public BaseIOEvent(final String type) {
            super(type);
        }

        public BaseIOEvent(final String type, final boolean sticky) {
            super(type, null, sticky);
        }

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

    /**
     *
     */
    public static class DataIOEvent extends BaseIOEvent {

        private static final long serialVersionUID = -1L;

        public final UUID srcTaskID;

        public final UUID dstTaskID;

        public DataIOEvent() {
            super(null, false);
            this.srcTaskID = null;
            this.dstTaskID = null;
        }

        public DataIOEvent(final String type, final UUID srcTaskID, final UUID dstTaskID) {
            this(type, srcTaskID, dstTaskID, false);
        }

        public DataIOEvent(final String type, final UUID srcTaskID, final UUID dstTaskID, final boolean sticky) {
            super(type, sticky);
            // sanity check.
            if (srcTaskID == null)
                throw new IllegalArgumentException("srcTaskID == null");
            if (dstTaskID == null)
                throw new IllegalArgumentException("dstTaskID == null");

            this.srcTaskID = srcTaskID;

            this.dstTaskID = dstTaskID;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("DataIOEvent = {")
                                        .append(" type = " + type + ", ")
                                        .append(" srcTaskID = " + srcTaskID.toString() + ", ")
                                        .append(" dstTaskID = " + dstTaskID.toString())
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static final class TransferBufferEvent extends DataIOEvent {

        private static final long serialVersionUID = -1;

        public final UUID messageID;

        // public final byte[] data;

        public final MemoryView buffer;

        public TransferBufferEvent(final UUID srcTaskID, final UUID dstTaskID, final MemoryView buffer) {
            this(UUID.randomUUID(), srcTaskID, dstTaskID, buffer);
        }

        public TransferBufferEvent(final UUID messageID, final UUID srcTaskID, final UUID dstTaskID, final MemoryView buffer) {

            super(DataEventType.DATA_EVENT_BUFFER, srcTaskID, dstTaskID);

            // sanity check.
            if (messageID == null)
                throw new IllegalArgumentException("messageID == null");
            if (buffer == null)
                throw new IllegalArgumentException("buffer == null");

            this.messageID = messageID;

            this.buffer = buffer;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("TransferBufferEvent = {")
                                        .append(" messageID = " + messageID.toString() + ", ")
                                        .append(" srcTaskID = " + srcTaskID.toString() + ", ")
                                        .append(" dstTaskID = " + dstTaskID.toString() + ", ")
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static class ControlIOEvent extends BaseIOEvent {

        private static final long serialVersionUID = -1L;

        private UUID srcMachineID;

        private UUID dstMachineID;

        public ControlIOEvent(final String type) {
            super(type);
        }

        public ControlIOEvent(final String type, final UUID srcMachineID, final UUID dstMachineID) {
            super(type);
            // sanity check.
            if (srcMachineID == null)
                throw new IllegalArgumentException("srcMachineID == null");
            if (dstMachineID == null)
                throw new IllegalArgumentException("dstMachineID == null");

            this.srcMachineID = srcMachineID;

            this.dstMachineID = dstMachineID;
        }

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
            return (new StringBuilder()).append("ControlIOEvent = {")
                                        .append(" type = " + type + ", ")
                                        .append(" srcMachineID = " + srcMachineID + ", ")
                                        .append(" dstMachineID = " + dstMachineID)
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static class TaskControlIOEvent extends ControlIOEvent {

        private static final long serialVersionUID = -1L;

        private UUID topologyID;

        private UUID taskID;

        public TaskControlIOEvent(final String type) {
            super(type);
        }

        public void setTopologyID(final UUID topologyID) {
            // sanity check.
            if (topologyID == null)
                throw new IllegalArgumentException("topologyID == null");

            this.topologyID = topologyID;
        }

        public UUID getTopologyID() {
            return this.topologyID;
        }

        public void setTaskID(final UUID taskID) {
            // sanity check.
            if (taskID == null)
                throw new IllegalArgumentException("taskID == null");

            this.taskID = taskID;
        }

        public UUID getTaskID() {
            return this.taskID;
        }
    }

    /**
     *
     */
    public static final class RPCCallerRequestEvent extends ControlIOEvent {

        private static final long serialVersionUID = -1L;

        public UUID callUID;

        public MethodSignature methodSignature;

        public RPCCallerRequestEvent(final UUID callUID, final MethodSignature methodSignature) {

            super(ControlEventType.CONTROL_EVENT_RPC_CALLER_REQUEST);
            // sanity check.
            if (callUID == null)
                throw new IllegalArgumentException("callUID == null");
            if (methodSignature == null)
                throw new IllegalArgumentException("methodSignature == null");

            this.callUID = callUID;

            this.methodSignature = methodSignature;
        }
    }

    /**
     *
     */
    public static final class RPCCalleeResponseEvent extends ControlIOEvent {

        private static final long serialVersionUID = -1L;

        public UUID callUID;

        public Object result;

        public RPCCalleeResponseEvent(final UUID callUID, final Object result) {

            super(ControlEventType.CONTROL_EVENT_RPC_CALLEE_RESPONSE);
            // sanity check.
            if (callUID == null)
                throw new IllegalArgumentException("callUID == null");

            this.callUID = callUID;

            this.result = result;
        }
    }

    // --------------------------------------------------
    // KRYO
}
