package de.tuberlin.aura.core.zookeeper;

import java.net.InetAddress;
import java.net.UnknownHostException;

import net.jcip.annotations.NotThreadSafe;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.*;

/**
 * This class wraps helper methods for interacting with ZooKeeper.
 * TODO: authors?
 */
@NotThreadSafe
public class ZkHelper {

	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory
		.getLogger(ZkHelper.class);

	/**
	 * ZooKeeper session timeout in ms.
	 */
	public static final int ZOOKEEPER_TIMEOUT = 10000;

	/**
	 * Root folder in ZooKeeper.
	 */
	public static final String ZOOKEEPER_ROOT = "/aura";

	/**
	 * Folder for the task-managers.
	 */
	public static final String ZOOKEEPER_TASKMANAGERS = ZOOKEEPER_ROOT
		+ "/taskmanagers";

	public static final String EVENT_TYPE_NODE_ADDED = "node_added";

	public static final String EVENT_TYPE_NODE_REMOVED = "node_removed";

	public static final String EVENT_TYPE_CONNECTION_EXPIRED = "connection_expired";

	private ZkHelper()
	{
		// This will never be called.
	}

	/**
	 * Initialize the directory structure in ZooKeeper.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void initDirectories(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		// Create the root folder of the aura application in ZooKeeper.
		Stat stat = zk.exists(ZOOKEEPER_ROOT, false);
		if (stat == null)
		{
			zk.create(ZOOKEEPER_ROOT, new byte[0],
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// Create a folder that is used to register the task-managers.
		stat = zk.exists(ZOOKEEPER_TASKMANAGERS, false);
		if (stat == null)
		{
			zk.create(ZOOKEEPER_TASKMANAGERS, new byte[0],
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	/**
	 * Check whether the format of the ZooKeeper connection string is valid.
	 * 
	 * @param zkServers
	 *        The connection string.
	 */
	public static void checkConnectionString(String zkServers) {

		checkNotNull(zkServers, "zkServers == null");

		String[] tokens = zkServers.split(";");
		for (String token : tokens)
		{
			String[] parts = token.split(":");
			try {
				InetAddress.getByName(parts[0]);
				int port = Integer.parseInt(parts[1]);
				checkArgument(port < 1024 || port > 65535, "Port {} is invalid", port);
			} catch (UnknownHostException e) {
				LOG.error("Could not find the ZooKeeper host: {}", parts[0]);
			} catch (NumberFormatException e) {
				LOG.error("Could not parse the port {}", parts[1]);
			}
		}
	}
}
