package de.tuberlin.aura.core.descriptors;

import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class Descriptors {

    // Disallow instantiation.
    private Descriptors() {
    }

    /**
     *
     */
    public static final class MachineDescriptor implements Serializable {

        private static final long serialVersionUID = -1L;

        public MachineDescriptor(InetAddress address, int dataPort, int controlPort, HardwareDescriptor hardware) {
            this(UUID.randomUUID(), address, dataPort, controlPort, hardware);
        }

        public MachineDescriptor(UUID uid, InetAddress address, int dataPort, int controlPort, HardwareDescriptor hardware) {
            // sanity check.
            if (uid == null)
                throw new IllegalArgumentException("uid == null");
            if (address == null)
                throw new IllegalArgumentException("address == null");
            if (dataPort < 1024 || dataPort > 65535)
                throw new IllegalArgumentException("dataPort invalid");
            if (controlPort < 1024 || controlPort > 65535)
                throw new IllegalArgumentException("controlPort invalid port number");
            if (hardware == null)
                throw new IllegalArgumentException("hardware == null");

            this.uid = uid;

            this.address = address;

            this.dataPort = dataPort;

            this.controlPort = controlPort;

            this.dataAddress = new InetSocketAddress(address, dataPort);

            this.controlAddress = new InetSocketAddress(address, controlPort);

            this.hardware = hardware;
        }

        public final UUID uid;

        public final int dataPort;

        public final int controlPort;

        public final InetAddress address;

        // TODO: redundant...
        public final InetSocketAddress dataAddress;

        public final InetSocketAddress controlAddress;

        public final HardwareDescriptor hardware;

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(uid.equals(((MachineDescriptor) other).uid)))
                return false;
            if (!(dataAddress.equals(((MachineDescriptor) other).dataAddress)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("MachineDescriptor = {")
                    .append(" uid = " + uid.toString())
                    .append(", ")
                    .append(" netAddress = " + dataAddress)
                    .append(", ")
                    .append(" controlAddress = " + controlAddress)
                    .append(", ")
                    .append(hardware)
                    .append(" }")
                    .toString();
        }
    }

    public static final class HardwareDescriptor implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = -1L;

        public HardwareDescriptor(int cpuCores, long sizeOfRAM, HDDDescriptor hdd) {
            this(UUID.randomUUID(), cpuCores, sizeOfRAM, hdd);
        }

        public HardwareDescriptor(UUID uid, int cpuCores, long sizeOfRAM, HDDDescriptor hdd) {
            // sanity check.
            if (uid == null)
                throw new IllegalArgumentException("uid == null");
            if (cpuCores < 1)
                throw new IllegalArgumentException("cpuCores < 1");
            if (sizeOfRAM < 1024 * 1024 * 1024)
                throw new IllegalArgumentException("Less than one GB of RAM");
            if (hdd == null)
                throw new IllegalArgumentException("hdd == null");

            this.uid = uid;
            this.cpuCores = cpuCores;
            this.sizeOfRAM = sizeOfRAM;
            this.hdd = hdd;
        }

        public final UUID uid;

        public final int cpuCores;

        public final long sizeOfRAM;

        public final HDDDescriptor hdd;

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(uid.equals(((MachineDescriptor) other).uid)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("HardwareDescriptor = {")
                    .append(" uid = " + uid.toString())
                    .append(", ")
                    .append(" cpuCores = " + cpuCores)
                    .append(", ")
                    .append(" sizeOfRAM = " + sizeOfRAM)
                    .append(", ")
                    .append(hdd)
                    .append(" }")
                    .toString();
        }
    }

    public static final class HDDDescriptor implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = -1L;

        public HDDDescriptor(long sizeOfHDD) {
            this(UUID.randomUUID(), sizeOfHDD);
        }

        public HDDDescriptor(UUID uid, long sizeOfHDD) {
            // sanity check.
            if (uid == null)
                throw new IllegalArgumentException("uid == null");
            if (sizeOfHDD < 1024 * 1024 * 1024)
                throw new IllegalArgumentException("Less than one GB of HDD space");

            this.uid = uid;
            this.sizeOfHDD = sizeOfHDD;
        }

        public final UUID uid;

        public final long sizeOfHDD;

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(uid.equals(((MachineDescriptor) other).uid)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("HDDDescriptor = {")
                    .append(" uid = " + uid.toString())
                    .append(", ")
                    .append(" sizeOfHDD = " + sizeOfHDD)
                    .append(" }")
                    .toString();
        }
    }

    /**
     *
     */
    public static final class TaskDescriptor implements Serializable {

        private static final long serialVersionUID = -1L;

        public TaskDescriptor(final UUID topologyID, final UUID taskID, final int taskIndex, final String name, final UserCode userCode) {
            // sanity check.
            if (topologyID == null)
                throw new IllegalArgumentException("topologyID == null");
            if (taskID == null)
                throw new IllegalArgumentException("taskID == null");
            if (taskIndex < 0)
                throw new IllegalArgumentException("taskIndex < 0");
            if (name == null)
                throw new IllegalArgumentException("name == null");
            if (userCode == null)
                throw new IllegalArgumentException("userCode == null");

            this.topologyID = topologyID;

            this.taskID = taskID;

            this.taskIndex = taskIndex;

            this.name = name;

            this.userCode = userCode;
        }

        public final UUID topologyID;

        public final UUID taskID;

        public final int taskIndex;

        public final String name;

        public final UserCode userCode;

        private MachineDescriptor machine;

        public void setMachineDescriptor(final MachineDescriptor machine) {
            // sanity check.
            if (machine == null)
                throw new IllegalArgumentException("machine == null");
            if (this.machine != null)
                throw new IllegalStateException("machine is already set");

            this.machine = machine;
        }

        public MachineDescriptor getMachineDescriptor() {
            return machine;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if ((machine == null && ((TaskDescriptor) other).machine == null) || !(machine.equals(((TaskDescriptor) other).machine)))
                return false;
            if (!(taskID.equals(((TaskDescriptor) other).taskID)))
                return false;
            if (!(name.equals(((TaskDescriptor) other).name)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("TaskDescriptor = {")
                    .append(" machine = " + machine.toString() + ", ")
                    .append(" uid = " + taskID.toString() + ", ")
                    .append(" name = " + name)
                    .append(" }")
                    .toString();
        }
    }

    /**
     *
     */
    public static final class TaskBindingDescriptor implements Serializable {

        private static final long serialVersionUID = -1L;

        public TaskBindingDescriptor(final TaskDescriptor task,
                                     final List<List<TaskDescriptor>> inputGateBindings,
                                     final List<List<TaskDescriptor>> outputGateBindings) {
            // sanity check.
            if (task == null)
                throw new IllegalArgumentException("taskID == null");
            if (inputGateBindings == null)
                throw new IllegalArgumentException("inputGateBindings == null");
            if (outputGateBindings == null)
                throw new IllegalArgumentException("outputGateBindings == null");

            this.task = task;

            this.inputGateBindings = Collections.unmodifiableList(inputGateBindings);

            this.outputGateBindings = Collections.unmodifiableList(outputGateBindings);
        }

        public final TaskDescriptor task;

        public final List<List<TaskDescriptor>> inputGateBindings;

        public final List<List<TaskDescriptor>> outputGateBindings;

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(task.equals(((TaskBindingDescriptor) other).task)))
                return false;
            if (!(inputGateBindings.equals(((TaskBindingDescriptor) other).inputGateBindings)))
                return false;
            if (!(outputGateBindings.equals(((TaskBindingDescriptor) other).outputGateBindings)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("TaskBindingDescriptor = {")
                    // .append( " task = " + task.toString() + ", " )
                    .append(" inputGates = " + inputGateBindings.toString() + ", ")
                    .append(" outputGates = " + outputGateBindings.toString())
                    .append(" }")
                    .toString();
        }
    }

    /**
     *
     */
    public static final class TaskDeploymentDescriptor implements Serializable {

        private static final long serialVersionUID = -1L;

        public TaskDeploymentDescriptor(final TaskDescriptor taskDescriptor,
                                        final TaskBindingDescriptor taskBindingDescriptor,
                                        final Node.DataPersistenceType dataPersistenceType,
                                        final Node.ExecutionType executionType) {

            // sanity check.
            if (taskDescriptor == null)
                throw new IllegalArgumentException("taskDescriptor == null");
            if (taskBindingDescriptor == null)
                throw new IllegalArgumentException("taskBindingDescriptor == null");
            if (dataPersistenceType == null)
                throw new IllegalArgumentException("dataPersistenceType == null");
            if (executionType == null)
                throw new IllegalArgumentException("executionType == null");

            this.taskDescriptor = taskDescriptor;

            this.taskBindingDescriptor = taskBindingDescriptor;

            this.dataPersistenceType = dataPersistenceType;

            this.executionType = executionType;
        }

        public final TaskDescriptor taskDescriptor;

        public final TaskBindingDescriptor taskBindingDescriptor;

        public final Node.DataPersistenceType dataPersistenceType;

        public final Node.ExecutionType executionType;

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(taskDescriptor.equals(((TaskDeploymentDescriptor) other).taskDescriptor)))
                return false;
            if (!(taskBindingDescriptor.equals(((TaskDeploymentDescriptor) other).taskBindingDescriptor)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("TaskDeploymentDescriptor = {")
                    .append(" taskDescriptor = " + taskDescriptor.toString() + ", ")
                    .append(" taskBindingDescriptor = " + taskBindingDescriptor.toString())
                    .append(" }")
                    .toString();
        }
    }
}
