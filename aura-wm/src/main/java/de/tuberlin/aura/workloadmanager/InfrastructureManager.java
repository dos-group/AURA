package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;


public final class InfrastructureManager extends EventDispatcher implements IInfrastructureManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final ZookeeperClient zookeeperClient;

    private final Map<UUID, MachineDescriptor> nodeMap;

    private List<MachineDescriptor> usedNodes;

    private Map<UUID, Queue<FileInputSplit>> inputSplitMap;

    private final Object monitor;

    private final InputSplitManager inputSplitManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final IWorkloadManager workloadManager, final String zookeeper, final MachineDescriptor wmMachine) {
        super();
        // sanity check.
        ZookeeperClient.checkConnectionString(zookeeper);

        if (workloadManager == null)
            throw new IllegalArgumentException("workloadManager == null");
        if (wmMachine == null)
            throw new IllegalArgumentException("wmMachine == null");

        this.monitor = new Object();

        this.nodeMap = new ConcurrentHashMap<>();

        this.usedNodes = new ArrayList<>();

        this.inputSplitMap = new ConcurrentHashMap<>();

        try {
            zookeeperClient = new ZookeeperClient(zookeeper);
            zookeeperClient.initDirectories();
            zookeeperClient.store(ZookeeperClient.ZOOKEEPER_WORKLOADMANAGER, wmMachine);

            // Get all available nodes.
            final ZookeeperTaskManagerWatcher watcher = new ZookeeperTaskManagerWatcher();

            synchronized (monitor) {
                final List<String> nodes = zookeeperClient.getChildrenForPathAndWatch(ZookeeperClient.ZOOKEEPER_TASKMANAGERS, watcher);
                for (final String node : nodes) {
                    final MachineDescriptor descriptor;
                    try {
                        descriptor = (MachineDescriptor) zookeeperClient.read(ZookeeperClient.ZOOKEEPER_TASKMANAGERS + "/" + node);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    this.nodeMap.put(descriptor.uid, descriptor);
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
    public synchronized MachineDescriptor getNextMachineForTask(final Topology.LogicalNode task) {

        synchronized (monitor) {

            // schedule tasks evenly on available machines
            // (TODO: manage actually available execution units per worker instead of this round-robin approach,
            // including errors when no slot is available)
            if (usedNodes.size() == nodeMap.size()) {
                usedNodes = new ArrayList<>();
            }

            final List<MachineDescriptor> workerMachines = new ArrayList<>(nodeMap.values());

            // try to schedule HDFS sources local to HDFS input splits
            // (TODO: translate inputSplit locations into locality preferences that the TopologyScheduler can manage
            // and provide to the InfrastructureManager so that getNextMachine(preferences) receives just preferences
            // and doesn't need to know about the specifics of e.g. HDFS sources and input splits. instead instances of
            // HDFS sources could each prefer the locations of one previously assigned split. this preferences mechanism
            // could then also be extended towards task co-location)
            if (task != null && task.isHDFSSource() && inputSplitMap.containsKey(task.uid)) {

                Queue<FileInputSplit> inputSplitsForTask = inputSplitMap.get(task.uid);

                if (!inputSplitsForTask.isEmpty()) {

                    int numberOfSplits = inputSplitsForTask.size();

                    // try to find an available machine which hosts an not yet assigned input split. when there is no
                    // such machine available queue the split again for a machine might become "available" before the
                    // next call (available (for evenly using machines) == max one more assignments than the others)
                    for (int i = 0; i < numberOfSplits; i++) {

                        FileInputSplit split = inputSplitsForTask.poll();
                        List<String> hostNames = Arrays.asList(split.getHostnames());

                        for (MachineDescriptor machine : workerMachines) {
                            if (!usedNodes.contains(machine) && hostNames.contains(machine.hostName)) {
                                usedNodes.add(machine);
                                return machine;
                            }
                        }

                        inputSplitsForTask.add(split);
                    }

                }
            }

            // round-robin scheduling (default / fall back)
            for (MachineDescriptor machine : workerMachines) {
                if (!usedNodes.contains(machine)) {
                    usedNodes.add(machine);
                    return machine;
                }
            }
        }

        return null;
    }

    @Override
    public void registerHDFSSource(final Topology.LogicalNode node) {

        inputSplitManager.registerHDFSSource(node);

        Queue<FileInputSplit> inputSplits = new LinkedList<>(inputSplitManager.getAllInputSplitsForLogicalHDFSSource(node));

        inputSplitMap.put(node.uid, inputSplits);
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

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class ZookeeperTaskManagerWatcher implements Watcher {

        @Override
        public synchronized void process(WatchedEvent event) {
            LOG.debug("Received event - internalState: {} - datasetType: {}", event.getState().toString(), event.getType().toString());

            try {
                synchronized (monitor) {
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
