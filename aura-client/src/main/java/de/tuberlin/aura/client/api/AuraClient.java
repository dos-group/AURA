package de.tuberlin.aura.client.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import de.tuberlin.aura.core.iosystem.spi.IIOManager;
import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.descriptors.DescriptorFactory;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.IClientWMProtocol;
import de.tuberlin.aura.core.taskmanager.usercode.UserCodeExtractor;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.AuraTopologyBuilder;
import de.tuberlin.aura.core.zookeeper.ZookeeperClient;

public final class AuraClient {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(AuraClient.class);

    public final IIOManager ioManager;

    public final IRPCManager rpcManager;

    public final IClientWMProtocol clientProtocol;

    public final UserCodeExtractor codeExtractor;

    public final Map<UUID, EventHandler> registeredTopologyMonitors;

    public final IORedispatcher ioHandler;

    public final UUID clientSessionID;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AuraClient(IConfig config) {
        final String zkServer = ZookeeperClient.buildServersString(config.getObjectList("zookeeper.servers"));

        // sanity check.
        ZookeeperClient.checkConnectionString(zkServer);

        final MachineDescriptor md = DescriptorFactory.createMachineDescriptor(config.getConfig("client"));

        this.ioManager = new IOManager(md, null, config.getConfig("client.io"));
        this.rpcManager = new RPCManager(ioManager, config.getConfig("client.io.rpc"));

        this.codeExtractor = new UserCodeExtractor(false);
        this.codeExtractor.addStandardDependency("java");
        this.codeExtractor.addStandardDependency("org/apache/log4j");
        this.codeExtractor.addStandardDependency("io/netty");
        this.codeExtractor.addStandardDependency("de/tuberlin/aura/core");

        final MachineDescriptor wmMachineDescriptor;
        try {
            ZookeeperClient client = new ZookeeperClient(zkServer);
            wmMachineDescriptor = (MachineDescriptor) client.read(ZookeeperClient.ZOOKEEPER_WORKLOADMANAGER);
            client.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        ioHandler = new IORedispatcher();
        ioManager.addEventListener(ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, ioHandler);
        ioManager.addEventListener(ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE, ioHandler);
        ioManager.connectMessageChannelBlocking(wmMachineDescriptor);

        clientProtocol = rpcManager.getRPCProtocolProxy(IClientWMProtocol.class, wmMachineDescriptor);

        this.registeredTopologyMonitors = new HashMap<>();
        // create examples session.
        this.clientSessionID = UUID.randomUUID();

        clientProtocol.openSession(clientSessionID);
        LOG.info("CLIENT IS READY");
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public AuraTopologyBuilder createTopologyBuilder() {
        return new AuraTopologyBuilder(ioManager.getMachineDescriptor().uid, codeExtractor);
    }


    public void submitTopology(final AuraTopology topology, final EventHandler handler) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        if (handler != null) {
            registeredTopologyMonitors.put(topology.topologyID, handler);
        }
        clientProtocol.submitTopology(clientSessionID, topology);
    }

    public void closeSession() {
        clientProtocol.closeSession(clientSessionID);
    }


    public void awaitSubmissionResult(final int numTopologies) {

        final CountDownLatch awaitExecution = new CountDownLatch(numTopologies);

        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                awaitExecution.countDown();
            }
        });

        ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                awaitExecution.countDown();
            }
        });

        try {
            awaitExecution.await();
        } catch (InterruptedException e) {
            LOG.error("latch was interrupted", e);
        }
    }

    public <E> Collection<E> getDataset(final UUID datasetID) {
        // sanity check.
        if (datasetID == null)
            throw new IllegalArgumentException("datasetID == null");

        return clientProtocol.getDataset(datasetID);
    }

    public <E> void broadcastDataset(final UUID datasetID, final Collection<E> dataset) {
        // sanity check.
        if (datasetID == null)
            throw new IllegalArgumentException("datasetID == null");
        if (dataset == null)
            throw new IllegalArgumentException("dataset == null");

        clientProtocol.broadcastDataset(datasetID, dataset);
    }

    public void eraseDataset(final UUID datasetID) {
        // sanity check.
        if (datasetID == null)
            throw new IllegalArgumentException("datasetID == null");
        clientProtocol.eraseDataset(datasetID);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class IORedispatcher extends EventHandler {

        @Handle(event = IOEvents.ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            LOG.info("Topology finished.");
        }

        @Handle(event = IOEvents.ControlIOEvent.class, type = ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE)
        private void handleTopologyFailure(final IOEvents.ControlIOEvent event) {
            LOG.info("Topology failed.");
        }
    }
}