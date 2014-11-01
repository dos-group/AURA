package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.aura.core.descriptors.Descriptors;
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

import static de.tuberlin.aura.workloadmanager.LocationPreference.PreferenceLevel;

public final class InfrastructureManager extends EventDispatcher implements IInfrastructureManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final ZookeeperClient zookeeperClient;

    private final IConfig config;

    // this monitor is used to protect the book keeping of available execution units (tmMachineMap and availableExecutionUnitsMap)
    private final Object nodeInfoMonitor;

    private final Map<UUID, MachineDescriptor> tmMachineMap;

    private final Map<UUID, Integer> availableExecutionUnitsMap;

    private int machineIdx;

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

        this.tmMachineMap = new ConcurrentHashMap<>();

        this.availableExecutionUnitsMap = new ConcurrentHashMap<>();

        this.machineIdx = 0;

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

                    LOG.info("ADDED MACHINE uid = " + descriptor.uid + " (" + descriptor.hostName + ")");

                    this.tmMachineMap.put(descriptor.uid, descriptor);
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

            final List<MachineDescriptor> workerMachines = new ArrayList<>(tmMachineMap.values());

            LOG.debug("--------- Available execution units before getMachine() call");

            for (int i = 0; i < workerMachines.size(); i++) {
                MachineDescriptor machine = workerMachines.get(i);
                LOG.debug(".. " + i + ". " + availableExecutionUnitsMap.get(machine.uid) + " available execution units on machine " + machine.uid + " [" + machine.hostName + "]");
            }

            if (locationPreference != null) {

                for (MachineDescriptor machine : locationPreference.preferredLocationAlternatives) {
                    if (availableExecutionUnitsMap.get(machine.uid) > 0) {
                        availableExecutionUnitsMap.put(machine.uid, availableExecutionUnitsMap.get(machine.uid) - 1);

                        LOG.debug("--------- Took machine " + machine.uid);

                        return machine;
                    }
                }

                if (locationPreference.preferenceLevel == PreferenceLevel.REQUIRED) {
                    throw new IllegalStateException("Could not schedule required co-location.");
                }
            }

            for (int i = 0; i < workerMachines.size(); i++) {

                machineIdx = (++machineIdx % workerMachines.size());

                MachineDescriptor machine = workerMachines.get(machineIdx);
                if (availableExecutionUnitsMap.get(machine.uid) > 0) {
                    availableExecutionUnitsMap.put(machine.uid, availableExecutionUnitsMap.get(machine.uid) - 1);

                    LOG.debug("--------- Took machine " + machine.uid + " with Index " + machineIdx);

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
    public void reclaimExecutionUnits(final Topology.AuraTopology finishedTopology) {
        synchronized (nodeInfoMonitor) {
            for (Topology.LogicalNode logicalNode : finishedTopology.nodeMap.values()) {
                for (Topology.ExecutionNode executionNode : logicalNode.getExecutionNodes()) {
                    if (!(executionNode.getNodeDescriptor() instanceof Descriptors.DatasetNodeDescriptor)) {
                        UUID machineID = executionNode.getNodeDescriptor().getMachineDescriptor().uid;
                        availableExecutionUnitsMap.put(machineID, availableExecutionUnitsMap.get(machineID) + 1);
                    }
                }
            }

        }
    }

    @Override
    public void reclaimExecutionUnits(final Topology.DatasetNode dataset) {
        // sanity check.
        if (dataset == null)
            throw new IllegalArgumentException("dataset == null");
        for (Topology.ExecutionNode executionNode : dataset.getExecutionNodes()) {
            UUID machineID = executionNode.getNodeDescriptor().getMachineDescriptor().uid;
            availableExecutionUnitsMap.put(machineID, availableExecutionUnitsMap.get(machineID) + 1);
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
        return tmMachineMap.values().size();
    }

    public List<MachineDescriptor> getMachinesWithInputSplit(InputSplit inputSplit) {

        final List<MachineDescriptor> machinesWithInputSplit = new ArrayList<>();
        final List<String> hostNames = Arrays.asList(inputSplit.getHostnames());

        synchronized (nodeInfoMonitor) {

            for (MachineDescriptor machine : tmMachineMap.values()) {
                if (hostNames.contains(machine.hostName)) {
                    machinesWithInputSplit.add(machine);
                }
            }
        }
        return machinesWithInputSplit;
    }

    public Map<UUID, Descriptors.MachineDescriptor> getTaskManagerMachines() {
        return Collections.unmodifiableMap(tmMachineMap);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class ZookeeperTaskManagerWatcher implements Watcher {

        @Override
        public synchronized void process(WatchedEvent event) {
            LOG.debug("Received event - state: {} - type: {}", event.getState().toString(), event.getType().toString());

            try {
                synchronized (nodeInfoMonitor) {
                    final List<String> nodeList = zookeeperClient.getChildrenForPath(ZookeeperClient.ZOOKEEPER_TASKMANAGERS);

                    // Find out whether a node was created or deleted.
                    if (tmMachineMap.values().size() < nodeList.size()) {

                        // A node has been added.
                        MachineDescriptor newMachine = null;
                        for (final String node : nodeList) {
                            final UUID machineID = UUID.fromString(node);
                            if (!tmMachineMap.containsKey(machineID)) {
                                newMachine = (MachineDescriptor) zookeeperClient.read(event.getPath() + "/" + node);
                                tmMachineMap.put(machineID, newMachine);
                                availableExecutionUnitsMap.put(machineID, config.getInt("tm.execution.units.number"));

                                LOG.info("ADDED MACHINE uid = " + machineID + " (" + newMachine.hostName + ")");
                            }
                        }

                        dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperClient.EVENT_TYPE_NODE_ADDED, newMachine));

                    } else {
                        // A node has been removed.
                        UUID machineID = null;
                        for (final UUID uid : tmMachineMap.keySet()) {
                            if (!nodeList.contains(uid.toString())) {
                                machineID = uid;
                                break;
                            }
                        }

                        final MachineDescriptor removedMachine = tmMachineMap.remove(machineID);
                        availableExecutionUnitsMap.remove(machineID);
                        if (removedMachine == null)
                            LOG.error("machine with uid = " + machineID + " can not be removed");
                        else
                            LOG.info("REMOVED MACHINE uid = " + machineID + " (" + removedMachine.hostName + ")");

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
