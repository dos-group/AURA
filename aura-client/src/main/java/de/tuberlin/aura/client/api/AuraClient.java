package de.tuberlin.aura.client.api;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;

public final class AuraClient {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public AuraClient(final String zkServer, int controlPort, int dataPort) {
		// sanity check.
		if (zkServer == null)
			throw new IllegalArgumentException("zkServer == null");
		if (dataPort < 1024 || dataPort > 65535)
			throw new IllegalArgumentException("dataPort invalid");
		if (controlPort < 1024 || controlPort > 65535)
			throw new IllegalArgumentException("controlPort invalid port number");

		final MachineDescriptor md = DescriptorFactory.getDescriptor(dataPort, controlPort);
		this.ioManager = new IOManager(md);
		this.rpcManager = new RPCManager(ioManager);

		this.codeExtractor = new UserCodeExtractor(true);
		this.codeExtractor.addStandardDependency("java").
			addStandardDependency("org/apache/log4j").
			addStandardDependency("io/netty").
			addStandardDependency("de/tuberlin/aura/core");

		final ZooKeeper zookeeper;
		final MachineDescriptor wmMachineDescriptor;
		try {
			zookeeper = new ZooKeeper(zkServer, ZkHelper.ZOOKEEPER_TIMEOUT,
				new ZkConnectionWatcher(new IEventHandler() {

					@Override
					public void handleEvent(Event event) {
					}
				}));

			wmMachineDescriptor = (MachineDescriptor) ZkHelper.readFromZookeeper(zookeeper,
				ZkHelper.ZOOKEEPER_WORKLOADMANAGER);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		ioManager.connectMessageChannelBlocking(wmMachineDescriptor);
		clientProtocol = rpcManager.getRPCProtocolProxy(ClientWMProtocol.class, wmMachineDescriptor);
		LOG.info("CLIENT IS READY");
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(AuraClient.class);

	public final IOManager ioManager;

	public final RPCManager rpcManager;

	public final ClientWMProtocol clientProtocol;

	public final UserCodeExtractor codeExtractor;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public AuraTopologyBuilder createTopologyBuilder() {
		return new AuraTopologyBuilder(codeExtractor);
	}

	public void submitTopology(final AuraTopology topology) {
		// sanity check.
		if (topology == null)
			throw new IllegalArgumentException("topology == null");

		clientProtocol.submitTopology(topology);
	}
}
