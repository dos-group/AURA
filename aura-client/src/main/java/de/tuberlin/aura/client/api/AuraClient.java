package de.tuberlin.aura.client.api;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.iosystem.IOEvents.MonitoringEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;
import de.tuberlin.aura.core.zookeeper.ZkConnectionWatcher;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class AuraClient {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class IORedispatcher extends EventHandler {

        @Handle(event = MonitoringEvent.class)
        private void handleMonitoringEvent(final MonitoringEvent event) {
            final EventHandler handler = registeredTopologyMonitors.get(event.topologyID);
            if (handler != null)
                handler.handleEvent(event);
        }
    }

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

		final MachineDescriptor md = DescriptorFactory.createMachineDescriptor(dataPort, controlPort);
		this.ioManager = new IOManager(md);
		this.rpcManager = new RPCManager(ioManager);

		this.codeExtractor = new UserCodeExtractor(false);
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

        ioHandler = new IORedispatcher();

        final String[] IOEvents = {MonitoringEvent.MONITORING_TOPOLOGY_STATE_EVENT,
                                   MonitoringEvent.MONITORING_TASK_STATE_EVENT};

        ioManager.addEventListener(IOEvents, ioHandler);

		ioManager.connectMessageChannelBlocking(wmMachineDescriptor);
		clientProtocol = rpcManager.getRPCProtocolProxy(ClientWMProtocol.class, wmMachineDescriptor);

        this.registeredTopologyMonitors = new HashMap<UUID,EventHandler>();

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

    public final Map<UUID,EventHandler> registeredTopologyMonitors;

    public final IORedispatcher ioHandler;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public AuraTopologyBuilder createTopologyBuilder() {
		return new AuraTopologyBuilder(ioManager.machine.uid, codeExtractor);
	}

	public void submitTopology(final AuraTopology topology, final EventHandler handler) {
		// sanity check.
		if (topology == null)
			throw new IllegalArgumentException("topology == null");

        if(handler != null) {
            registeredTopologyMonitors.put(topology.topologyID, handler);
        }
		clientProtocol.submitTopology(topology);
	}
}
