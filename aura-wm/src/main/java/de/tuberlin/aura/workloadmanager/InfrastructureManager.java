package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;
import de.tuberlin.aura.core.config.IConfig;

import javax.crypto.Mac;

import static de.tuberlin.aura.workloadmanager.LocationPreference.PreferenceLevel;

public final class InfrastructureManager extends EventDispatcher implements IInfrastructureManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final ZookeeperClient zookeeperClient;

    private final IConfig config;

    // this monitor is used to protect the book keeping of available execution units (nodeMap and availableExecutionUnitsMap)
    private final Object nodeInfoMonitor;

    private final Map<UUID, MachineDescriptor> nodeMap;

    private final Map<UUID, Integer> availableExecutionUnitsMap;

    private final InputSplitManager inputSplitManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final IWorkloadManager workloadManager, final String zookeeper, final MachineDescriptor wmMachine, IConfig config) {
        super();
        // sanity check.
        ZookeeperClient.checkConnectionString(zookeeper);

        if (workloadManager == null)
            throw new IllegalArgumentException("workloadManager == null");
        if (wmMachine == null)
            throw new IllegalArgumentException("wmMachine == null");

        this.config = config;

        this.nodeInfoMonitor = new Object();

        this.nodeMap = new ConcurrentHashMap<>();

        this.availableExecutionUnitsMap = new ConcurrentHashMap<>();

        try {
            zookeeperClient = new ZookeeperClient(zookeeper);
            zookeeperClient.initDirectories();
            zookeeperClient.store(ZookeeperClient.ZOOKEEPER_WORKLOADMANAGER, wmMachine);

            // Get all available nodes.
            final ZookeeperTaskManagerWatcher watcher = new ZookeeperTaskManagerWatcher();

            // FIXME: this synchronization in the constructor isn't necessary, is it?
            synchronized (nodeInfoMonitor) {
                final List<String> nodes = zookeeperClient.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_TASKMANAGERS, watcher);
                for (final String node : nodes) {
                    final MachineDescriptor descriptor;
                    try {
                        descriptor = (MachineDescriptor) zookeeperClient.read(ZookeeperClient.ZOOKEEPER_TASKMANAGERS + "/" + node);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    this.nodeMap.put(descriptor.uid, descriptor);
                    this.availableExecutionUnitsMap.put(descriptor.uid, config.getInt("tm.execution.units.number"));
                }
            }

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        this.inputSplitManager = new InputSplitManager(workloadManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized MachineDescriptor getMachine(LocationPreference locationPreference) {

        synchronized (nodeInfoMonitor) {

            final List<MachineDescriptor> workerMachines = new ArrayList<>(nodeMap.values());

            if (locationPreference != null) {

                for (MachineDescriptor machine : locationPreference.preferredLocationAlternatives) {
                    if (availableExecutionUnitsMap.get(machine.uid) > 0) {
                        availableExecutionUnitsMap.put(machine.uid, availableExecutionUnitsMap.get(machine.uid) - 1);
                        return machine;
                    }
                }

                if (locationPreference.preferenceLevel == PreferenceLevel.REQUIRED) {
                    throw new IllegalStateException("Could not schedule required co-location.");
                }
            }

            for (MachineDescriptor machine : workerMachines) {

                if (availableExecutionUnitsMap.get(machine.uid) > 0) {
                    availableExecutionUnitsMap.put(machine.uid, availableExecutionUnitsMap.get(machine.uid) - 1);
                    return machine;
                }

            }

            // TODO: handle this case (and the above exception) in the TopologyScheduler/WorkloadManager by waiting for
            // resources to become available (but maybe then reason about the scheduling some more: deploy from sources
            // so partially deployed queries can progress as much as possible)
            throw new IllegalStateException("No execution unit available");

        }

    }

    @Override
    public void reclaimExecutionUnits(Topology.AuraTopology finishedTopology) {

        synchronized (nodeInfoMonitor) {

            for (Topology.LogicalNode logicalNode : finishedTopology.nodeMap.values()) {
                for (Topology.ExecutionNode executionNode : logicalNode.getExecutionNodes()) {
                    UUID machineID = executionNode.getNodeDescriptor().getMachineDescriptor().uid;
                    availableExecutionUnitsMap.put(machineID, availableExecutionUnitsMap.get(machineID) + 1);
                }
            }

        }
    }

    @Override
    public List<InputSplit> registerHDFSSource(final Topology.LogicalNode node) {
        return inputSplitManager.registerHDFSSource(node);
    }

    @Override
    public void shutdownInfrastructureManager() {
        zookeeperClient.close();
        shutdownEventDispatcher();
        LOG.info("shutdownEventDispatcher infrastructure manager");
    }

    @Override
    public synchronized InputSplit getNextInputSplitForHDFSSource(final Topology.ExecutionNode executionNode) {
        return inputSplitManager.getNextInputSplitForExecutionUnit(executionNode);
    }

    @Override
    public synchronized int getNumberOfMachines() {
        return nodeMap.values().size();
    }

    public List<MachineDescriptor> getMachinesWithInputSplit(InputSplit inputSplit) {

        List<MachineDescriptor> machinesWithInputSplit = new ArrayList<>();

        List<String> hostNames = Arrays.asList(inputSplit.getHostnames());

        synchronized (nodeInfoMonitor) {

            for (MachineDescriptor machine : nodeMap.values()) {
                if (hostNames.contains(machine.hostName)) {
                    machinesWithInputSplit.add(machine);
                }
            }
        }

        return machinesWithInputSplit;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class ZookeeperTaskManagerWatcher implements Watcher {

        @Override
        public synchronized void process(WatchedEvent event) {
            LOG.debug("Received event - internalState: {} - datasetType: {}", event.getState().toString(), event.getType().toString());

            try {
                synchronized (nodeInfoMonitor) {
                    final List<String> nodeList = zookeeperClient.getChildrenForPath(ZookeeperClient.ZOOKEEPER_TASKMANAGERS);

                    // Find out whether a node was created or deleted.
                    if (nodeMap.values().size() < nodeList.size()) {

                        // A node has been added.
                        MachineDescriptor newMachine = null;
                        for (final String node : nodeList) {
                            final UUID machineID = UUID.fromString(node);
                            if (!nodeMap.containsKey(machineID)) {
                                newMachine = (MachineDescriptor) zookeeperClient.read(event.getPath() + "/" + node);
                                nodeMap.put(machineID, newMachine);
                                availableExecutionUnitsMap.put(machineID, config.getInt("tm.execution.units.number"));
                            }
                        }

                        dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperClient.EVENT_TYPE_NODE_ADDED, newMachine));

                    } else {
                        // A node has been removed.
                        UUID machineID = null;
                        for (final UUID uid : nodeMap.keySet()) {
                            if (!nodeList.contains(uid.toString())) {
                                machineID = uid;
                                break;
                            }
                        }

                        final MachineDescriptor removedMachine = nodeMap.remove(machineID);
                        availableExecutionUnitsMap.remove(machineID);
                        if (removedMachine == null)
                            LOG.error("machine with uid = " + machineID + " can not be removed");
                        else
                            LOG.info("REMOVED MACHINE uid = " + machineID);

                        dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperClient.EVENT_TYPE_NODE_REMOVED, removedMachine));
                    }
                }

                // keep watching
                zookeeperClient.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_TASKMANAGERS, this);

            } catch (InterruptedException e) {
                LOG.error("Interaction with ZooKeeper was interrupted", e);
            } catch (Exception e) {
                LOG.error("ZooKeeper operation failed", e);
            }
        }
    }
}
