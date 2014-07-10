package de.tuberlin.aura.workloadmanager;

import java.util.*;

import net.jcip.annotations.NotThreadSafe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZookeeperHelper;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

/**
 *
 */
@NotThreadSafe
public class InfrastructureManager extends EventDispatcher implements IInfrastructureManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(InfrastructureManager.class);

    /**
     * The only instance of this class.
     */
    private static InfrastructureManager INSTANCE;

    /**
     * The connection to the ZooKeeper-cluster.
     */
    private CuratorFramework zookeeperClient;

    /**
     * Stores all task manager nodes.
     */
    private final Map<UUID, MachineDescriptor> nodeMap;

    private final MachineDescriptor wmMachine;

    private int machineIdx;

    private final Object taskManagerMonitor = new Object();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * Constructor.
     * 
     * @param zkServer This string must contain the connection to the ZooKeeper-cluster.
     */
    private InfrastructureManager(final String zkServer, final MachineDescriptor wmMachine) {
        super();
        // sanity check.
        ZookeeperHelper.checkConnectionString(zkServer);
        if (wmMachine == null)
            throw new IllegalArgumentException("wmMachine == null");

        this.wmMachine = wmMachine;

        this.nodeMap = new HashMap<>();

        try {
            zookeeperClient = CuratorFrameworkFactory.newClient(zkServer, new ExponentialBackoffRetry(1000, 3));
            zookeeperClient.start();

            ZookeeperHelper.initDirectories(this.zookeeperClient);

            ZookeeperHelper.storeInZookeeper(zookeeperClient, ZookeeperHelper.ZOOKEEPER_WORKLOADMANAGER, this.wmMachine);

            // Get all available nodes.
            final ZkTaskManagerWatcher watcher = new ZkTaskManagerWatcher();

            synchronized (taskManagerMonitor) {
                final List<String> nodes = zookeeperClient.getChildren().usingWatcher(watcher).forPath(ZookeeperHelper.ZOOKEEPER_TASKMANAGERS);

                for (final String node : nodes) {
                    final MachineDescriptor descriptor;
                    try {
                        descriptor = (MachineDescriptor) ZookeeperHelper.readFromZookeeper(zookeeperClient, ZookeeperHelper.ZOOKEEPER_TASKMANAGERS + "/" + node);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    this.nodeMap.put(descriptor.uid, descriptor);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * Get an instance of the infrastructure manager.
     * 
     * @param zkServers This string must contain the connection to the ZooKeeper-cluster.
     */
    public static InfrastructureManager getInstance(final String zkServers, final MachineDescriptor wmMachine) {
        if (INSTANCE == null) {
            INSTANCE = new InfrastructureManager(zkServers, wmMachine);
        }
        return INSTANCE;
    }

    public synchronized MachineDescriptor getNextMachine() {
        final List<MachineDescriptor> workerMachines = new ArrayList<>(nodeMap.values());
        final MachineDescriptor md = workerMachines.get(machineIdx);
        machineIdx = (++machineIdx % workerMachines.size());
        return md;
    }

    public synchronized int getNumberOfMachine() {
        return nodeMap.values().size();
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class ZkTaskManagerWatcher implements Watcher {

        /**
         * {@inheritDoc}
         */
        @Override
        public synchronized void process(WatchedEvent event) {
            LOG.debug("Received event - state: {} - type: {}", event.getState().toString(), event.getType().toString());

            try {

                synchronized (taskManagerMonitor) {
                    final List<String> nodeList = zookeeperClient.getChildren().forPath(ZookeeperHelper.ZOOKEEPER_TASKMANAGERS);

                    // Find out whether a node was created or deleted.
                    if (nodeMap.values().size() < nodeList.size()) {
                        // A node has been added.
                        MachineDescriptor newMachine = null;
                        for (final String node : nodeList) {
                            final UUID machineID = UUID.fromString(node);
                            if (!nodeMap.containsKey(machineID)) {
                                newMachine = (MachineDescriptor) ZookeeperHelper.readFromZookeeper(zookeeperClient, event.getPath() + "/" + node);
                                nodeMap.put(machineID, newMachine);
                            }

                        }

                        dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperHelper.EVENT_TYPE_NODE_ADDED, newMachine));

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

                        dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(ZookeeperHelper.EVENT_TYPE_NODE_REMOVED, removedMachine));
                    }
                }

                // keep watching
                zookeeperClient.getChildren().usingWatcher(this).forPath(ZookeeperHelper.ZOOKEEPER_TASKMANAGERS);

            } catch (InterruptedException e) {
                LOG.error("Interaction with ZooKeeper was interrupted", e);
            } catch (Exception e) {
                LOG.error("ZooKeeper operation failed", e);
            }
        }
    }
}
