package de.tuberlin.aura.workloadmanager;

import java.io.IOException;

import net.jcip.annotations.NotThreadSafe;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkConstants;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

/**
 * Singleton.
 * 
 * TODO: Where to define event types?
 */
@NotThreadSafe
public class InfrastructureManager extends EventDispatcher implements
		IInfrastructureManager
{
	/**
	 * Logger.
	 */
	private static final Logger LOG = Logger
			.getLogger(InfrastructureManager.class);

	/**
	 * The only instance of this class.
	 */
	private static InfrastructureManager INSTANCE;

	/**
	 * The connection to the ZooKeeper-cluster.
	 */
	private ZooKeeper zk;

	public static final String EVENT_TYPE_NODE_AVAILABLE = "node_available";

	/**
	 * Constructor.
	 * 
	 * @param zkServers
	 *           This string must contain the connection to the
	 *           ZooKeeper-cluster.
	 */
	private InfrastructureManager(String zkServers)
	{
		// TODO: Sanity check of the connection string.
		try
		{
			this.zk = new ZooKeeper(zkServers, ZkConstants.ZOOKEEPER_TIMEOUT,
					new ZkConnectionWatcher());
			initDirectories();
		}
		catch (IOException e)
		{
			LOG.error("Couldn't connect to ZooKeeper", e);
		}
		catch (KeeperException e)
		{
			LOG.error("An error occurred in ZooKeeper", e);
		}
		catch (InterruptedException e)
		{
			LOG.error("The connection to ZooKeeper was interrupted.", e);
		}
	}

	/**
	 * Get an instance of the infrastructure manager.
	 * 
	 * @param zkServers
	 *           This string must contain the connection to the
	 *           ZooKeeper-cluster.
	 */
	public static InfrastructureManager getInstance(String zkServers)
	{
		if (INSTANCE == null)
		{
			INSTANCE = new InfrastructureManager(zkServers);
		}

		return INSTANCE;
	}

	/**
	 * Initialize the directory structure in ZooKeeper.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void initDirectories() throws KeeperException, InterruptedException
	{
		// Create the root folder of the aura application in ZooKeeper.
		Stat stat = this.zk.exists(ZkConstants.ZOOKEEPER_ROOT, false);
		if (stat == null)
		{
			this.zk.create(ZkConstants.ZOOKEEPER_ROOT, new byte[0],
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// Create a folder that is used to register the task-managers.
		stat = this.zk.exists(ZkConstants.ZOOKEEPER_TASKMANAGERS, false);
		if (stat == null)
		{
			this.zk.create(ZkConstants.ZOOKEEPER_TASKMANAGERS, new byte[0],
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
}
