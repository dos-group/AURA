package de.tuberlin.aura.core.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;

import net.jcip.annotations.NotThreadSafe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps helper methods for interacting with ZooKeeper.
 */
@NotThreadSafe
public class ZookeeperHelper {

    // Disallow instantiation.
    private ZookeeperHelper() {}

    // ---------------------------------------------------
    // Zookeeper Event Constants.
    // ---------------------------------------------------

    public static final String EVENT_TYPE_NODE_ADDED = "node_added";

    public static final String EVENT_TYPE_NODE_REMOVED = "node_removed";

    public static final String EVENT_TYPE_CONNECTION_ESTABLISHED = "connection_established";

    public static final String EVENT_TYPE_CONNECTION_EXPIRED = "connection_expired";


    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperHelper.class);

    /**
     * Root folder in ZooKeeper.
     */
    public static final String ZOOKEEPER_ROOT = "/aura";

    /**
     * Folder for the task-managers.
     */
    public static final String ZOOKEEPER_TASKMANAGERS = ZOOKEEPER_ROOT + "/taskmanagers";

    /**
     * Folder for the workload-manager.
     */
    public static final String ZOOKEEPER_WORKLOADMANAGER = ZOOKEEPER_ROOT + "/workloadmanager";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void initDirectories(CuratorFramework zookeeperClient) throws Exception {
        // Create the root folder of the aura application in ZooKeeper.
        Stat stat = zookeeperClient.checkExists().forPath(ZOOKEEPER_ROOT);
        if (stat == null) {
            try {
                zookeeperClient.create().forPath(ZOOKEEPER_ROOT, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }

        // Create a folder that is used to register the task-managers.
        stat = zookeeperClient.checkExists().forPath(ZOOKEEPER_TASKMANAGERS);
        if (stat == null) {
            try {
                zookeeperClient.create().forPath(ZOOKEEPER_TASKMANAGERS, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }
    }

    public static Object readFromZookeeper(final CuratorFramework client, final String path) throws Exception {
        try {
            final byte[] data = client.getData().forPath(path);
            final ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(data);
            final ObjectInputStream objectStream = new ObjectInputStream(byteArrayStream);
            return objectStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param zookeeperClient
     * @param path
     * @param object
     */
    public static void storeInZookeeper(final CuratorFramework zookeeperClient, final String path, final Object object) throws Exception {
        try {
            final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            final ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
            objectStream.writeObject(object);
            objectStream.flush();
            zookeeperClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, byteArrayStream.toByteArray());
            objectStream.close();
            byteArrayStream.close();
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    /**
     * Check whether the format of the ZooKeeper connection string is valid.
     *
     * @param zkServer The connection string.
     */
    public static void checkConnectionString(String zkServer) {
        checkNotNull(zkServer, "zkServers == null");
        final String[] tokens = zkServer.split(";");
        for (String token : tokens) {
            final String[] parts = token.split(":");
            try {
                final int port = Integer.parseInt(parts[1]);
                checkArgument(port > 1024 && port < 65535, "Port {} is invalid", port);
            } catch (NumberFormatException e) {
                LOG.error("Could not parse the port {}", parts[1]);
            }
        }
    }

}
