package de.tuberlin.aura.tests.clients;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.demo.examples.ExampleTopologies;


public class IntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

    private static AuraClient auraClient;

    private static int nodes;

    private static int cores;

    private static int executionUnits;

    @BeforeClass
    public static void setup() {
        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);
        switch (simConfig.getString("simulator.mode")) {
            case "local":
                new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                LOG.warn("'simulator mode' has unknown value. Fallback to local mode.");
                new LocalClusterSimulator(simConfig);
        }

        auraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        nodes = simConfig.getInt("simulator.tm.number");
        cores = simConfig.getInt("tm.execution.units.number");

        executionUnits = nodes * cores;

        LOG.info("start test with: " + nodes + " nodes and " + cores + " cores per node");
    }

    @AfterClass
    public static void tearDown() {
        auraClient.closeSession();
    }

    // --------------------------------------------------
    // TESTS
    // --------------------------------------------------

    @Test
    public void testMinimalTopology() {
        runTopology(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits));
    }

    @Test
    public void testMultiQuery() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits / 2));
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits / 2));
        runTopologiesConcurrently(topologies);
    }

    @Test
    public void testExtendedTopology() {
        runTopology(ExampleTopologies.six_layer_all2all(auraClient, executionUnits));
    }

    @Test
    public void testMultipleQueries() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();

        // 2 layered - all2all connection
        topologies.add(ExampleTopologies.two_layer_point2point_small(auraClient, executionUnits));

        // 3 layered - point2point + point2point connection
        topologies.add(ExampleTopologies.three_layer_point2point(auraClient, executionUnits));

        // 3 layered - all2all + point2point connection
        topologies.add(ExampleTopologies.three_layer_all2all_point2point(auraClient, executionUnits));

        // 3 layered - point2point + all2all connection
        topologies.add(ExampleTopologies.three_layer_point2point_all2all(auraClient, executionUnits));

        // 3 layered - all2all + all2all connection
        topologies.add(ExampleTopologies.three_layer_all2all_all2all(auraClient, executionUnits));

        // 3 layered - point2point (join) point2point connection
        topologies.add(ExampleTopologies.three_layer_point2point_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) point2point connection
        topologies.add(ExampleTopologies.three_layer_all2all_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection
        topologies.add(ExampleTopologies.three_layer_all2all_join_all2all(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection (small/large)
        topologies.add(ExampleTopologies.three_layer_all2all_join_all2all_sl(auraClient, executionUnits));

        runTopologies(topologies);
    }

    // --------------------------------------------------
    // HELPER METHODS
    // --------------------------------------------------

    private static void runTopology(Topology.AuraTopology topology) {
        List<Topology.AuraTopology> topologies = Collections.singletonList(topology);

        runTopologies(topologies);
    }

    private static void runTopologies(List<Topology.AuraTopology> topologies) {
        SubmissionHandler handler = new SubmissionHandler(auraClient, topologies, 1);
        TopologyFinishedHandler finishedHandler = new TopologyFinishedHandler();

        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);
        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, finishedHandler);

        handler.handleTopologyFinished(null);

        auraClient.awaitSubmissionResult(topologies.size());

        auraClient.ioManager.removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);
        auraClient.ioManager.removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, finishedHandler);
    }

    private static void runTopologiesConcurrently(final List<Topology.AuraTopology> topologies) {
        TopologyFinishedHandler handler = new TopologyFinishedHandler();

        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);

        for (int i = 0; i < topologies.size(); i++) {
            final int jobIndex = i;

            new Thread() {

                public void run() {
                    LOG.info("Submit: {}", topologies.get(jobIndex).name);
                    auraClient.submitTopology(topologies.get(jobIndex), null);
                }
            }.start();
        }

        auraClient.awaitSubmissionResult(topologies.size());
        auraClient.ioManager.removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);
    }

    private static class SubmissionHandler extends EventHandler {

        private final AuraClient client;

        private final List<Topology.AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        public SubmissionHandler(final AuraClient client, final List<Topology.AuraTopology> topologies, final int runs) {
            this.client = client;
            this.topologies = topologies;
            this.runs = runs;
        }

        @Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            if (jobCounter < runs * topologies.size()) {
                final int jobIndex = jobCounter++ / runs;

                Thread t = new Thread() {

                    public void run() {
                        LOG.info("Submit: {} - run {}/{}", topologies.get(jobIndex).name, ((jobCounter - 1) % runs) + 1, runs);
                        client.submitTopology(topologies.get(jobIndex), null);
                    }
                };

                t.start();
            }
        }
    }

    private static class TopologyFinishedHandler extends EventHandler {

        @Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            if (event != null) {
                String jobName = (String) event.getPayload();
                LOG.info("Topology ({}) finished.", jobName);
            }
        }
    }
}
