package de.tuberlin.aura.core.iosystem;

import java.io.Serializable;
import java.util.UUID;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.iosystem.RPCManager.MethodSignature;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyState;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyTransition;
import io.netty.channel.Channel;

public final class IOEvents {

    // Disallow instantiation.
    private IOEvents() {}

    /**
     *
     */
    public static final class DataEventType {

        private DataEventType() {}

        public static final String DATA_EVENT_INPUT_CHANNEL_CONNECTED = "DATA_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_CHANNEL_CONNECTED = "DATA_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String DATA_EVENT_OUTPUT_GATE_OPEN = "DATA_EVENT_OUTPUT_GATE_OPEN";

        public static final String DATA_EVENT_OUTPUT_GATE_CLOSE = "DATA_EVENT_OUTPUT_GATE_CLOSE";

        public static final String DATA_EVENT_BUFFER = "DATA_EVENT_BUFFER";

        public static final String DATA_EVENT_SOURCE_EXHAUSTED = "DATA_EVENT_SOURCE_EXHAUSTED";

        public static final String DATA_EVENT_OUTPUT_GATE_CLOSE_FINISHED = "DATA_EVENT_OUTPUT_GATE_CLOSE_FINISHED";
    }

    /**
     *
     */
    public static final class ControlEventType {

        private ControlEventType() {}

        public static final String CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_OUTPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_INPUT_CHANNEL_CONNECTED = "CONTROL_EVENT_INPUT_CHANNEL_CONNECTED";

        public static final String CONTROL_EVENT_MESSAGE = "CONTROL_EVENT_MESSAGE";

        public static final String CONTROL_EVENT_RPC_CALLER_REQUEST = "CONTROL_EVENT_RPC_CALLER_REQUEST";

        public static final String CONTROL_EVENT_RPC_CALLEE_RESPONSE = "CONTROL_EVENT_RPC_CALLEE_RESPONSE";

        public static final String CONTROL_EVENT_TASK_STATE = "CONTROL_EVENT_TASK_STATE";

        public static final String CONTROL_EVENT_INCOMPLETE_EVENT = "CONTROL_EVENT_INCOMPLETE_EVENT";

        public static final String CONTROL_EVENT_TOPOLOGY_FINISHED = "CONTROL_EVENT_TOPOLOGY_FINISHED";

        public static final String CONTROL_EVENT_TOPOLOGY_FAILURE = "CONTROL_EVENT_TOPOLOGY_FAILURE";
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
            return (new StringBuilder()).append("GenericIOEvent = {")
                                        .append(" type = " + type + ", ")
                                        .append(" payload = " + payload.toString() + ", ")
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static class DataIOEvent extends BaseIOEvent {

        private static final long serialVersionUID = 1L;

        public DataIOEvent(final String type, final UUID srcTaskID, final UUID dstTaskID) {
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
    public static final class DataBufferEvent extends DataIOEvent {

        private static final long serialVersionUID = -1;

        public DataBufferEvent(final UUID srcTaskID, final UUID dstTaskID, final byte[] data) {
            this(UUID.randomUUID(), srcTaskID, dstTaskID, data);
        }

        public DataBufferEvent(final UUID messageID, final UUID srcTaskID, final UUID dstTaskID, final byte[] data) {

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
            return (new StringBuilder()).append("DataBufferMessage = {")
                                        .append(" messageID = " + messageID.toString() + ", ")
                                        .append(" srcTaskID = " + srcTaskID.toString() + ", ")
                                        .append(" dstTaskID = " + dstTaskID.toString() + ", ")
                                        .append(" data = " + data + ", ")
                                        .append(" length( data ) = " + data.length)
                                        .append(" }")
                                        .toString();
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
            return (new StringBuilder()).append("ControlIOEvent = {")
                                        .append(" type = " + type + ", ")
                                        .append(" srcMachineID = " + srcMachineID.toString() + ", ")
                                        .append(" dstMachineID = " + dstMachineID.toString())
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static final class RPCCallerRequestEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

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

        public UUID callUID;

        public MethodSignature methodSignature;
    }

    /**
     *
     */
    public static final class RPCCalleeResponseEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public RPCCalleeResponseEvent(final UUID callUID, final Object result) {

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
    public static final class TaskStateTransitionEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public static final String TASK_STATE_TRANSITION_EVENT = "TASK_STATE_TRANSITION_EVENT";

        public TaskStateTransitionEvent(final TaskTransition transition) {
            this(null, null, transition);
        }

        public TaskStateTransitionEvent(final UUID topologyID, final UUID taskID, final TaskTransition transition) {
            super(TASK_STATE_TRANSITION_EVENT);
            // sanity check.
            if (transition == null)
                throw new IllegalArgumentException("taskTransition == null");

            this.topologyID = topologyID;

            this.taskID = taskID;

            this.transition = transition;
        }

        public final TaskTransition transition;

        public final UUID topologyID;

        public final UUID taskID;

        @Override
        public String toString() {
            return (new StringBuilder()).append("TaskChangeStateEvent = {")
                                        .append(" type = " + super.type + ", ")
                                        .append(" taskID = " + topologyID + ", ")
                                        .append(" taskTransition = " + transition.toString())
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static final class MonitoringEvent extends ControlIOEvent {

        private static final long serialVersionUID = 1L;

        public static final String MONITORING_TASK_STATE_EVENT = "MONITORING_TASK_STATE_EVENT";

        public static final String MONITORING_TOPOLOGY_STATE_EVENT = "MONITORING_TOPOLOGY_STATE_EVENT";

        public static final class TaskStateUpdate implements Serializable {

            private static final long serialVersionUID = 1L;

            public TaskStateUpdate(final TaskStateUpdate taskStateUpdate) {
                this(taskStateUpdate.taskID,
                     taskStateUpdate.name,
                     taskStateUpdate.currentTaskState,
                     taskStateUpdate.nextTaskState,
                     taskStateUpdate.taskTransition,
                     taskStateUpdate.stateDuration);
            }

            public TaskStateUpdate(final UUID taskID,
                                   final String name,
                                   final TaskState currentTaskState,
                                   final TaskState nextTaskState,
                                   final TaskTransition taskTransition,
                                   final long stateDuration) {
                // sanity check.
                if (taskID == null)
                    throw new IllegalArgumentException("taskID == null");
                if (name == null)
                    throw new IllegalArgumentException("name == null");
                if (currentTaskState == null)
                    throw new IllegalArgumentException("currentTaskState == null");
                if (nextTaskState == null)
                    throw new IllegalArgumentException("nextTaskState == null");
                if (taskTransition == null)
                    throw new IllegalArgumentException("taskTransition == null");

                this.taskID = taskID;

                this.name = name;

                this.currentTaskState = currentTaskState;

                this.nextTaskState = nextTaskState;

                this.taskTransition = taskTransition;

                this.stateDuration = stateDuration;
            }

            public final UUID taskID;

            public final String name;

            public final TaskState currentTaskState;

            public final TaskState nextTaskState;

            public final TaskTransition taskTransition;

            public final long stateDuration;
        }

        public static final class TopologyStateUpdate implements Serializable {

            private static final long serialVersionUID = 1L;


            public TopologyStateUpdate(final TopologyStateUpdate topologyStateUpdate) {
                this(topologyStateUpdate.name,
                     topologyStateUpdate.currentTopologyState,
                     topologyStateUpdate.nextTopologyState,
                     topologyStateUpdate.topologyTransition,
                     topologyStateUpdate.stateDuration);
            }

            public TopologyStateUpdate(final String name,
                                       final TopologyState currentTopologyState,
                                       final TopologyState nextTopologyState,
                                       final TopologyTransition topologyTransition,
                                       final long stateDuration) {
                // sanity check.
                if (name == null)
                    throw new IllegalArgumentException("name == null");
                if (currentTopologyState == null)
                    throw new IllegalArgumentException("currentTopologyState == null");
                if (nextTopologyState == null)
                    throw new IllegalArgumentException("nextTopologyState == null");
                if (topologyTransition == null)
                    throw new IllegalArgumentException("topologyTransition == null");

                this.name = name;

                this.currentTopologyState = currentTopologyState;

                this.nextTopologyState = nextTopologyState;

                this.topologyTransition = topologyTransition;

                this.stateDuration = stateDuration;
            }

            public final String name;

            public final TopologyState currentTopologyState;

            public final TopologyState nextTopologyState;

            public final TopologyTransition topologyTransition;

            public final long stateDuration;
        }

        public MonitoringEvent(final MonitoringEvent monitoringEvent) {
            this(monitoringEvent.type, UUID.fromString(monitoringEvent.topologyID.toString()), monitoringEvent.taskStateUpdate != null
                    ? new TaskStateUpdate(monitoringEvent.taskStateUpdate)
                    : null, monitoringEvent.topologyStateUpdate != null ? new TopologyStateUpdate(monitoringEvent.topologyStateUpdate) : null);
        }

        public MonitoringEvent(final UUID topologyID, final TaskStateUpdate taskStateUpdate) {
            this(MONITORING_TASK_STATE_EVENT, topologyID, taskStateUpdate, null);
        }


        public MonitoringEvent(final UUID topologyID, final TopologyStateUpdate topologyStateUpdate) {
            this(MONITORING_TOPOLOGY_STATE_EVENT, topologyID, null, topologyStateUpdate);
        }

        public MonitoringEvent(final String type,
                               final UUID topologyID,
                               final TaskStateUpdate taskStateUpdate,
                               final TopologyStateUpdate topologyStateUpdate) {
            super(type);
            // sanity check.
            if (topologyID == null)
                throw new IllegalArgumentException("topologyID == null");

            this.topologyID = topologyID;

            this.taskStateUpdate = taskStateUpdate;

            this.topologyStateUpdate = topologyStateUpdate;
        }

        public final UUID topologyID;

        public TaskStateUpdate taskStateUpdate;

        public TopologyStateUpdate topologyStateUpdate;
    }
}
