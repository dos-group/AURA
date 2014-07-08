package de.tuberlin.aura.core.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;

import net.jcip.annotations.NotThreadSafe;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
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
     * ZooKeeper session timeout in ms.
     */
    public static final int ZOOKEEPER_TIMEOUT = 5000;

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

    /**
     * Initialize the directory structure in ZooKeeper.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void initDirectories(ZooKeeper zk) throws KeeperException, InterruptedException {
        // Create the root folder of the aura application in ZooKeeper.
        Stat stat = zk.exists(ZOOKEEPER_ROOT, false);
        if (stat == null) {
            try {
                zk.create(ZOOKEEPER_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // No need to worry about this. You only want those nodes to exists. No matter who
                // created them.
            }
        }

        // Create a folder that is used to register the task-managers.
        stat = zk.exists(ZOOKEEPER_TASKMANAGERS, false);
        if (stat == null) {
            try {
                zk.create(ZOOKEEPER_TASKMANAGERS, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // No need to worry about this. You only want those nodes to exists. No matter who
                // created them.
            }
        }
    }

    /**
     * Check whether the format of the ZooKeeper connection string is valid.
     * 
     * @param zkServers The connection string.
     */
    public static void checkConnectionString(String zkServers) {
        checkNotNull(zkServers, "zkServers == null");
        final String[] tokens = zkServers.split(";");
        for (String token : tokens) {
            final String[] parts = token.split(":");
            try {
                // InetAddress.getByName(parts[0]);
                final int port = Integer.parseInt(parts[1]);
                checkArgument(port > 1024 && port < 65535, "Port {} is invalid", port);
                // } catch (UnknownHostException e) {
                // LOG.error("Could not find the ZooKeeper host: {}", parts[0]);
            } catch (NumberFormatException e) {
                LOG.error("Could not parse the port {}", parts[1]);
            }
        }
    }

    /**
     * @param zk
     * @param dir
     * @param object
     */
    public static void storeInZookeeper(final ZooKeeper zk, final String dir, final Object object) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
            zk.create(dir, baos.toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            oos.close();
            baos.close();
        } catch (KeeperException | IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    /**
     * @param zk
     * @param dir
     * @return
     */
    public static Object readFromZookeeper(final ZooKeeper zk, final String dir) {
        try {
            final byte[] data = zk.getData(dir, false, null);
            final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            final ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (KeeperException | IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
            return null;
        }
    }
}
