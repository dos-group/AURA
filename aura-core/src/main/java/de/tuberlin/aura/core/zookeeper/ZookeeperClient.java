package de.tuberlin.aura.core.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
import java.util.List;

import net.jcip.annotations.NotThreadSafe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.config.IConfig;

@NotThreadSafe
public class ZookeeperClient {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String EVENT_TYPE_NODE_ADDED = "node_added";

    public static final String EVENT_TYPE_NODE_REMOVED = "node_removed";

    public static final String EVENT_TYPE_CONNECTION_ESTABLISHED = "connection_established";

    public static final String EVENT_TYPE_CONNECTION_EXPIRED = "connection_expired";

    public static final String ZOOKEEPER_ROOT = "/aura";

    public static final String ZOOKEEPER_TASKMANAGERS = ZOOKEEPER_ROOT + "/taskmanagers";

    public static final String ZOOKEEPER_WORKLOADMANAGER = ZOOKEEPER_ROOT + "/workloadmanager";

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);


    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private CuratorFramework curator;


    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ZookeeperClient(String zkServer) {
        curator = CuratorFrameworkFactory.newClient(zkServer, 60000, 60000, new ExponentialBackoffRetry(1000, 3));
        curator.start();
    }


    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void initDirectories() throws Exception {
        // Create the root folder of the aura application in ZooKeeper.
        Stat stat = curator.checkExists().forPath(ZOOKEEPER_ROOT);
        if (stat == null) {
            try {
                curator.create().forPath(ZOOKEEPER_ROOT, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }

        // Create a folder that is used to register the taskmanager-managers.
        stat = curator.checkExists().forPath(ZOOKEEPER_TASKMANAGERS);
        if (stat == null) {
            try {
                curator.create().forPath(ZOOKEEPER_TASKMANAGERS, new byte[0]);
            } catch (KeeperException.NodeExistsException e) {
                // These nodes only need to exist. No matter who created them.
            }
        }
    }

    public Object read(final String path) throws Exception {
        try {
            final byte[] data = curator.getData().forPath(path);
            final ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(data);
            final ObjectInputStream objectStream = new ObjectInputStream(byteArrayStream);
            return objectStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public void store(final String path, final Object object) throws Exception {
        try {
            final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
            final ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
            objectStream.writeObject(object);
            objectStream.flush();
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, byteArrayStream.toByteArray());
            objectStream.close();
            byteArrayStream.close();
        } catch (InterruptedException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    public List<String> getChildrenForPath(String path) {
        try {
            return curator.getChildren().forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public List<String> getChildrenForPathAndWatch(String path, Watcher watcher) {
        try {
            return curator.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() {
        curator.close();
    }

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

    public static String buildServersString(List<? extends IConfig> servers) {
        StringBuilder sb = new StringBuilder();
        for (IConfig server : servers) {
            sb.append(server.getString("host"));
            sb.append(':');
            sb.append(server.getInt("port"));
            sb.append(';');
        }
        return servers.isEmpty() ? "" : sb.substring(0, sb.length() - 1);
    }
}
