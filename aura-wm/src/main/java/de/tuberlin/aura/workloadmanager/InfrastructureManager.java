package de.tuberlin.aura.workloadmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import net.jcip.annotations.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

/**
 * Singleton.
 * 
 * TODO: Where to define event types?
 */
@NotThreadSafe
public class InfrastructureManager extends EventDispatcher implements
		IEventHandler, IInfrastructureManager {
	/**
	 * Logger.
	 */
	private static final Logger LOG = LoggerFactory
			.getLogger(InfrastructureManager.class);

	/**
	 * The only instance of this class.
	 */
	private static InfrastructureManager INSTANCE;

	/**
	 * The connection to the ZooKeeper-cluster.
	 */
	private ZooKeeper zk;

	private HashMap<UUID, MachineDescriptor> nodeMap;

	/**
	 * Constructor.
	 * 
	 * @param zkServers
	 *            This string must contain the connection to the
	 *            ZooKeeper-cluster.
	 */
	private InfrastructureManager(String zkServers) {

		super();

		this.nodeMap = new HashMap<UUID, MachineDescriptor>();

		// TODO: Sanity check of the connection string.
		ZkHelper.checkConnectionString(zkServers);

		try {
			// Get a connection to ZooKeeper and initialize the directories in
			// ZooKeeper.
			this.zk = new ZooKeeper(zkServers, ZkHelper.ZOOKEEPER_TIMEOUT,
					new ZkConnectionWatcher(this));
			ZkHelper.initDirectories(this.zk);

			// Get all available nodes.
			ZkTaskManagerWatcher watcher = new ZkTaskManagerWatcher(this,
					this.zk);
			synchronized (watcher) {
				List<String> nodes = this.zk.getChildren(
						ZkHelper.ZOOKEEPER_TASKMANAGERS, watcher);
				for (String node : nodes) {
					try {
						byte[] data = this.zk.getData(
								ZkHelper.ZOOKEEPER_TASKMANAGERS + "/" + node,
								false, null);
						ByteArrayInputStream bais = new ByteArrayInputStream(
								data);
						ObjectInputStream ois = new ObjectInputStream(bais);
						MachineDescriptor descriptor = (MachineDescriptor) ois
								.readObject();
						this.nodeMap.put(descriptor.uid, descriptor);
						
						watcher.nodes.add(descriptor.uid.toString());
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException e) {
			LOG.error("Couldn't connect to ZooKeeper", e);
		} catch (KeeperException e) {
			LOG.error("An error occurred in ZooKeeper", e);
		} catch (InterruptedException e) {
			LOG.error("The connection to ZooKeeper was interrupted.", e);
		}
	}

	/**
	 * Get an instance of the infrastructure manager.
	 * 
	 * @param zkServers
	 *            This string must contain the connection to the
	 *            ZooKeeper-cluster.
	 */
	public static InfrastructureManager getInstance(String zkServers) {
		if (INSTANCE == null) {
			INSTANCE = new InfrastructureManager(zkServers);
		}

		return INSTANCE;
	}

	@Override
	public void addEventListener(final String type, final IEventHandler listener)
	{
		super.addEventListener(type, listener);
		
		switch (type) {
		case ZkHelper.EVENT_TYPE_NODE_ADDED:
			// Return all available nodes immediately.
			for(MachineDescriptor curDescriptor : this.nodeMap.values())
			{
				Event event = new Event(type, curDescriptor);
				handleEvent(event);
			}
			
			break;
		}
	}

	@Override
	public void handleEvent(Event event) {
		switch (event.type) {
		case ZkHelper.EVENT_TYPE_CONNECTION_EXPIRED:
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				LOG.error("ZooKeeper operation was interrupted", e);
			}
		case ZkHelper.EVENT_TYPE_NODE_ADDED:
			MachineDescriptor descriptor = (MachineDescriptor) event.data;
			this.nodeMap.put(descriptor.uid, descriptor);

			dispatchEvent(event);
			break;
		case ZkHelper.EVENT_TYPE_NODE_REMOVED:
			UUID nodeID = (UUID) event.data;
			event = new Event(event.type, this.nodeMap.get(nodeID));
			dispatchEvent(event);
			break;
		default:
			dispatchEvent(event);
		}
	}
}