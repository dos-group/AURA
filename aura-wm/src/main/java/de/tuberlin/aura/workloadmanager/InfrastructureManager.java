package de.tuberlin.aura.workloadmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import net.jcip.annotations.NotThreadSafe;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

@NotThreadSafe
public class InfrastructureManager extends EventDispatcher implements
		IInfrastructureManager {

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	private class ZkTaskManagerWatcher implements Watcher {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public synchronized void process(WatchedEvent event) {
			LOG.debug("received event: " + event.getState().toString());
			LOG.debug("received event: " + event.getType().toString());

			try {
				switch (event.getType()) {
				case NodeChildrenChanged:
					// Find out whether a node was created or deleted.
					final List<String> nodeList = zookeeper.getChildren(ZkHelper.ZOOKEEPER_TASKMANAGERS, false);

					if (nodeMap.values().size() < nodeList.size()) {
						// A node has been added.
						MachineDescriptor newMachine = null;
						for (final String node : nodeList) {
							final UUID machineID = UUID.fromString(node);
							if (!nodeMap.containsKey(machineID)) {
								newMachine = (MachineDescriptor) ZkHelper.readFromZookeeper(zookeeper,
									event.getPath() + "/" + node);
								nodeMap.put(machineID, newMachine);
								break;
							}
						}

						dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(
							ZkHelper.EVENT_TYPE_NODE_ADDED, newMachine));

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

						dispatchEvent(new de.tuberlin.aura.core.common.eventsystem.Event(
							ZkHelper.EVENT_TYPE_NODE_REMOVED, removedMachine));
					}

					break;
				default:
					// Nothing to do here.
				}

				// Stay interested in changes within the worker folder.
				zookeeper.getChildren(ZkHelper.ZOOKEEPER_TASKMANAGERS, this);
			} catch (KeeperException e) {
				LOG.error("ZooKeeper operation failed", e);
			} catch (InterruptedException e) {
				LOG.error("Interaction with ZooKeeper was interrupted", e);
			}
		}
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	/** Logger. */
	private static final Logger LOG = LoggerFactory
		.getLogger(InfrastructureManager.class);

	/** The only instance of this class. */
	private static InfrastructureManager INSTANCE;

	/** The connection to the ZooKeeper-cluster. */
	private ZooKeeper zookeeper;

	/** Stores all task manager nodes. */
	private final HashMap<UUID, MachineDescriptor> nodeMap;

	private final MachineDescriptor wmMachine;

	private int machineIdx;

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------
	/**
	 * Constructor.
	 *
	 * @param zkServers
	 *        This string must contain the connection to the
	 *        ZooKeeper-cluster.
	 */
	private InfrastructureManager(final String zkServers, final MachineDescriptor wmMachine) {
		super();
		// sanity check.
		ZkHelper.checkConnectionString(zkServers);
		if (wmMachine == null)
			throw new IllegalArgumentException("wmMachine == null");

		this.wmMachine = wmMachine;

		this.nodeMap = new HashMap<UUID, MachineDescriptor>();

		try {

			final IEventHandler eh = new IEventHandler() {
				@Override
				public synchronized void handleEvent(Event event) {
					switch (event.type) {
					case ZkHelper.EVENT_TYPE_CONNECTION_EXPIRED:
						try {
							zookeeper.close();
						} catch (InterruptedException e) {
							LOG.error("ZooKeeper operation was interrupted", e);
						}
					}
				}
			};

			// Get a connection to ZooKeeper and initialize
			// the directories in ZooKeeper.
			this.zookeeper = new ZooKeeper(zkServers, ZkHelper.ZOOKEEPER_TIMEOUT, new ZkConnectionWatcher(eh));
			ZkHelper.initDirectories(this.zookeeper);
			// Store the workload manager machine descriptor.
			ZkHelper.storeInZookeeper(zookeeper, ZkHelper.ZOOKEEPER_WORKLOADMANAGER, this.wmMachine);

			// Get all available nodes.
			final ZkTaskManagerWatcher watcher = new ZkTaskManagerWatcher();
			synchronized (watcher) {
				final List<String> nodes = this.zookeeper.getChildren(ZkHelper.ZOOKEEPER_TASKMANAGERS, watcher);
				for (final String node : nodes) {
					final MachineDescriptor descriptor = (MachineDescriptor) ZkHelper.readFromZookeeper(zookeeper,
						ZkHelper.ZOOKEEPER_TASKMANAGERS + "/" + node);
					this.nodeMap.put(descriptor.uid, descriptor);
				}
			}
		} catch (IOException | KeeperException e) {
			throw new IllegalStateException(e);
		} catch (InterruptedException e) {
			LOG.error("The connection to ZooKeeper was interrupted.", e);
		}
	}

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	/**
	 * Get an instance of the infrastructure manager.
	 *
	 * @param zkServers
	 *        This string must contain the connection to the
	 *        ZooKeeper-cluster.
	 */
	public static InfrastructureManager getInstance(final String zkServers, final MachineDescriptor wmMachine) {
		if (INSTANCE == null) {
			INSTANCE = new InfrastructureManager(zkServers, wmMachine);
		}
		return INSTANCE;
	}

	public synchronized MachineDescriptor getNextMachine() {
		final List<MachineDescriptor> workerMachines = new ArrayList<MachineDescriptor>(nodeMap.values());
		final MachineDescriptor md = workerMachines.get(machineIdx);
		machineIdx = (++machineIdx % workerMachines.size());
		return md;
	}
}
