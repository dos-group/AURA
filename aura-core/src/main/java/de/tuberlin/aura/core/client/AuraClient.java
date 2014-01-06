package de.tuberlin.aura.core.client;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;

public final class AuraClient {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public AuraClient(final MachineDescriptor clientMachine, final MachineDescriptor workloadManager) {
		// sanity check.
		if (clientMachine == null)
			throw new IllegalArgumentException("clientMachine == null");
		if (workloadManager == null)
			throw new IllegalArgumentException("workloadManager == null");

		LOG.info("CLIENT STARTED");

		this.ioManager = new IOManager(clientMachine);
		this.rpcManager = new RPCManager(ioManager);

		this.codeExtractor = new UserCodeExtractor(true);
		this.codeExtractor.addStandardDependency("java").
			addStandardDependency("org/apache/log4j").
			addStandardDependency("io/netty").
			addStandardDependency("de/tuberlin/aura/core");

		ioManager.connectMessageChannelBlocking(workloadManager);
		clientProtocol = rpcManager.getRPCProtocolProxy(ClientWMProtocol.class, workloadManager);
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
