package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.core.filesystem.InputSplitManager;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;


public final class InfrastructureManager extends EventDispatcher implements IInfrastructureManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    private final ZookeeperClient zookeeperClient;

    private final Map<UUID, MachineDescriptor> nodeMap;

    private int machineIdx;

    private final Object monitor;

    private final InputSplitManager inputSplitManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InfrastructureManager(final String zkServer, final MachineDescriptor wmMachine) {
        super();
        // sanity check.
        ZookeeperClient.checkConnectionString(zkServer);
        if (wmMachine == null)
            throw new IllegalArgumentException("wmMachine == null");

        this.monitor = new Object();

        this.nodeMap = new ConcurrentHashMap<>();

        try {
            zookeeperClient = new ZookeeperClient(zkServer);
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

        this.inputSplitManager = new InputSplitManager();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public synchronized MachineDescriptor getNextMachine() {
        final List<MachineDescriptor> workerMachines = new ArrayList<>(nodeMap.values());
        final MachineDescriptor md = workerMachines.get(machineIdx);
        machineIdx = (++machineIdx % workerMachines.size());
        return md;
    }

    @Override
    public synchronized InputSplit getInputSplitFromHDFSSource(final Topology.ExecutionNode executionNode) {
        return inputSplitManager.getInputSplitFromHDFSSource(executionNode);
    }

    @Override
    public void registerHDFSSource(final Topology.LogicalNode node) {
        inputSplitManager.registerHDFSSource(node);
    }

    @Override
    public synchronized int getNumberOfMachine() {
        return nodeMap.values().size();
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class ZookeeperTaskManagerWatcher implements Watcher {

        @Override
        public synchronized void process(WatchedEvent event) {
            LOG.debug("Received event - state: {} - type: {}", event.getState().toString(), event.getType().toString());

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
