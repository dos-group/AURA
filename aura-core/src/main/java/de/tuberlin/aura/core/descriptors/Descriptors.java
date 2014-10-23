package de.tuberlin.aura.core.descriptors;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.taskmanager.usercode.UserCode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;

public final class Descriptors {

    // Disallow instantiation.
    private Descriptors() {}

    /**
     *
     */
    public static final class MachineDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final UUID uid;

        public final int dataPort;

        public final int controlPort;

        public final InetAddress address;

        // TODO: redundant...
        public final InetSocketAddress dataAddress;

        public final InetSocketAddress controlAddress;

        public final HardwareDescriptor hardware;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

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

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + dataPort;
            result = 31 * result + controlPort;
            result = 31 * result + address.hashCode();
            result = 31 * result + dataAddress.hashCode();
            result = 31 * result + controlAddress.hashCode();
            return result;
        }

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

    /**
     *
     */
    public static final class HardwareDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final UUID uid;

        public final int cpuCores;

        public final long sizeOfRAM;

        public final HDDDescriptor hdd;


        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public HardwareDescriptor(int cpuCores, long sizeOfRAM, HDDDescriptor hdd) {
            this(UUID.randomUUID(), cpuCores, sizeOfRAM, hdd);
        }

        public HardwareDescriptor(UUID uid, int cpuCores, long sizeOfRAM, HDDDescriptor hdd) {
            // sanity check.
            if (uid == null)
                throw new IllegalArgumentException("uid == null");
            if (cpuCores < 1)
                throw new IllegalArgumentException("cpuCores < 1");
            if (sizeOfRAM < 1)
                throw new IllegalArgumentException("sizeOfRAM < 1");
            if (hdd == null)
                throw new IllegalArgumentException("hdd == null");

            this.uid = uid;
            this.cpuCores = cpuCores;
            this.sizeOfRAM = sizeOfRAM;
            this.hdd = hdd;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

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

    /**
     *
     */
    public static final class HDDDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final UUID uid;

        public final long sizeOfHDD;

        public HDDDescriptor(long sizeOfHDD) {
            this(UUID.randomUUID(), sizeOfHDD);
        }

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public HDDDescriptor(UUID uid, long sizeOfHDD) {
            // sanity check.
            if (uid == null)
                throw new IllegalArgumentException("uid == null");
            if (sizeOfHDD < 1024 * 1024 * 1024)
                throw new IllegalArgumentException("Less than one GB of HDD space");

            this.uid = uid;
            this.sizeOfHDD = sizeOfHDD;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

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
    public static class AbstractNodeDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public UUID topologyID;

        public final UUID taskID;

        public final int taskIndex;

        public final String name;

        public final List<UserCode> userCodeList;

        private MachineDescriptor machine;

        private List<Class<?>> userCodeClasses;

        public final List<DataflowNodeProperties> propertiesList;

        public boolean isReExecutable;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractNodeDescriptor(final UUID topologyID,
                                      final UUID taskID,
                                      final int taskIndex,
                                      final String name,
                                      final List<UserCode> userCodeList,
                                      final DataflowNodeProperties properties,
                                      final boolean isReExecutable) {

            this(topologyID, taskID, taskIndex, name, userCodeList, Arrays.asList(properties), isReExecutable);
        }

        public AbstractNodeDescriptor(final UUID topologyID,
                                      final UUID taskID,
                                      final int taskIndex,
                                      final String name,
                                      final List<UserCode> userCodeList,
                                      final List<DataflowNodeProperties> propertiesList,
                                      final boolean isReExecutable) {
            // sanity check.
            if (topologyID == null)
                throw new IllegalArgumentException("topologyID == null");
            if (taskID == null)
                throw new IllegalArgumentException("taskID == null");
            if (taskIndex < 0)
                throw new IllegalArgumentException("taskIndex < 0");
            if (name == null)
                throw new IllegalArgumentException("name == null");

            this.topologyID = topologyID;

            this.taskID = taskID;

            this.taskIndex = taskIndex;

            this.name = name;

            this.userCodeList = userCodeList;

            this.userCodeClasses = null;

            this.propertiesList = propertiesList;

            this.isReExecutable = isReExecutable;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

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

        public void setUserCodeClasses(final List<Class<?>> userCodeClasses) {
            // sanity check.
            if (userCodeClasses == null)
                throw new IllegalArgumentException("userCodeClasses == null");

            this.userCodeClasses = userCodeClasses;
        }

        public List<Class<?>> getUserCodeClasses() {
            return Collections.unmodifiableList(userCodeClasses);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if ((machine == null && ((AbstractNodeDescriptor) other).machine == null) || !(machine.equals(((AbstractNodeDescriptor) other).machine)))
                return false;
            if (!(taskID.equals(((AbstractNodeDescriptor) other).taskID)))
                return false;
            if (!(name.equals(((AbstractNodeDescriptor) other).name)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("AbstractNodeDescriptor = {")
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
    public static final class DatasetNodeDescriptor extends AbstractNodeDescriptor {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public AbstractDataset.DatasetType datasetType;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public DatasetNodeDescriptor(final UUID topologyID,
                                     final UUID taskID,
                                     final int taskIndex,
                                     final String name,
                                     final DataflowNodeProperties properties,
                                     final boolean isReExecutable,
                                     final AbstractDataset.DatasetType datasetType) {

            super(topologyID, taskID, taskIndex, name, null, properties, isReExecutable);

            this.datasetType = datasetType;
        }
    }

    /**
     *
     */
    public static final class OperatorNodeDescriptor extends AbstractNodeDescriptor {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public OperatorNodeDescriptor(final UUID topologyID,
                                      final UUID taskID,
                                      final int taskIndex,
                                      final String name,
                                      final List<UserCode> userCodeList,
                                      final DataflowNodeProperties properties,
                                      final boolean isReExecutable) {

            super(topologyID, taskID, taskIndex, name, userCodeList, properties, isReExecutable);
        }

        public OperatorNodeDescriptor(final UUID topologyID,
                                      final UUID taskID,
                                      final int taskIndex,
                                      final String name,
                                      final List<UserCode> userCodeList,
                                      final List<DataflowNodeProperties> propertiesList,
                                      final boolean isReExecutable) {

            super(topologyID, taskID, taskIndex, name, userCodeList, propertiesList, isReExecutable);
        }
    }

    /**
     *
     */
    public static final class InvokeableNodeDescriptor extends AbstractNodeDescriptor {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public InvokeableNodeDescriptor(final UUID topologyID,
                                        final UUID taskID,
                                        final int taskIndex,
                                        final String name,
                                        final List<UserCode> userCodeList,
                                        final boolean isReExecutable) {

            super(topologyID, taskID, taskIndex, name, userCodeList, (DataflowNodeProperties)null, isReExecutable);
        }
    }

    /**
     *
     */
    public static final class NodeBindingDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final AbstractNodeDescriptor task;

        public final List<List<AbstractNodeDescriptor>> inputGateBindings;

        public final List<List<AbstractNodeDescriptor>> outputGateBindings;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public NodeBindingDescriptor(final AbstractNodeDescriptor task,
                                     final List<List<AbstractNodeDescriptor>> inputGateBindings,
                                     final List<List<AbstractNodeDescriptor>> outputGateBindings) {
            // sanity check.
            if (task == null)
                throw new IllegalArgumentException("taskID == null");
            if (inputGateBindings == null)
                throw new IllegalArgumentException("inputGateBindings == null");
            if (outputGateBindings == null)
                throw new IllegalArgumentException("outputGateBindings == null");

            this.task = task;

            this.inputGateBindings = inputGateBindings;

            this.outputGateBindings = outputGateBindings;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addOutputGateBinding(final List<List<AbstractNodeDescriptor>> outputGateBindings) {
            // sanity check.
            if (outputGateBindings == null)
                throw new IllegalArgumentException("outputGateBindings == null");

            this.outputGateBindings.addAll(outputGateBindings);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(task.equals(((NodeBindingDescriptor) other).task)))
                return false;
            if (!(inputGateBindings.equals(((NodeBindingDescriptor) other).inputGateBindings)))
                return false;
            if (!(outputGateBindings.equals(((NodeBindingDescriptor) other).outputGateBindings)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("NodeBindingDescriptor = {")
            // .append( " taskmanager = " + taskmanager.toString() + ", " )
                                        .append(" inputGates = " + inputGateBindings.toString() + ", ")
                                        .append(" outputGates = " + outputGateBindings.toString())
                                        .append(" }")
                                        .toString();
        }
    }

    /**
     *
     */
    public static final class DeploymentDescriptor implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        public final AbstractNodeDescriptor nodeDescriptor;

        public final NodeBindingDescriptor nodeBindingDescriptor;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public DeploymentDescriptor(final AbstractNodeDescriptor nodeDescriptor,
                                    final NodeBindingDescriptor nodeBindingDescriptor) {
            // sanity check.
            if (nodeDescriptor == null)
                throw new IllegalArgumentException("nodeDescriptor == null");
            if (nodeBindingDescriptor == null)
                throw new IllegalArgumentException("nodeBindingDescriptor == null");

            this.nodeDescriptor = nodeDescriptor;

            this.nodeBindingDescriptor = nodeBindingDescriptor;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other == null)
                return false;
            if (other.getClass() != getClass())
                return false;

            if (!(nodeDescriptor.equals(((DeploymentDescriptor) other).nodeDescriptor)))
                return false;
            if (!(nodeBindingDescriptor.equals(((DeploymentDescriptor) other).nodeBindingDescriptor)))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("DeploymentDescriptor = {")
                                        .append(" nodeDescriptor = " + nodeDescriptor.toString() + ", ")
                                        .append(" nodeBindingDescriptor = " + nodeBindingDescriptor.toString())
                                        .append(" }")
                                        .toString();
        }
    }
}
