package de.tuberlin.aura.tests.util;

import java.util.Collections;
import java.util.List;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.topology.Topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TestHelper.class);

    public static void runTopology(AuraClient auraClient, Topology.AuraTopology topology) {
        List<Topology.AuraTopology> topologies = Collections.singletonList(topology);

        runTopologies(auraClient, topologies);
    }

    public static void runTopologies(AuraClient auraClient, List<Topology.AuraTopology> topologies) {
        SubmissionHandler handler = new SubmissionHandler(auraClient, topologies, 1);
        TopologyFinishedHandler finishedHandler = new TopologyFinishedHandler();

        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);
        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, finishedHandler);

        handler.handleTopologyFinished(null);

        auraClient.awaitSubmissionResult(topologies.size());

        auraClient.ioManager.removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);
        auraClient.ioManager.removeEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, finishedHandler);
    }

    public static void runTopologiesConcurrently(final AuraClient auraClient, final List<Topology.AuraTopology> topologies) {
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
