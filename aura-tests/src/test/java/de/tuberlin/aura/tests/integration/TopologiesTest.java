package de.tuberlin.aura.tests.integration;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.tests.util.TestHelper;


public final class TopologiesTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologiesTest.class);

    private static AuraClient auraClient;

    private static LocalClusterSimulator clusterSimulator = null;

    private static int executionUnits;

    // --------------------------------------------------
    // Tests.
    // --------------------------------------------------

    @BeforeClass
    public static void setup() {
        IConfig simConfig = IConfigFactory.load(IConfig.Type.SIMULATOR);
        switch (simConfig.getString("simulator.mode")) {
            case "LOCAL":
                clusterSimulator = new LocalClusterSimulator(simConfig);
                break;
            case "cluster":
                break;
            default:
                LOG.warn("'simulator mode' has unknown value. Fallback to LOCAL mode.");
                clusterSimulator = new LocalClusterSimulator(simConfig);
        }

        auraClient = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));

        int nodes = simConfig.getInt("simulator.tm.number");
        int cores = simConfig.getInt("tm.execution.units.number");

        executionUnits = nodes * cores;

        if (executionUnits < 8) {
            throw new IllegalStateException("TopologiesTest requires at least 8 execution units.");
        }

        LOG.info("start test with: " + nodes + " nodes and " + cores + " cores per node");
    }

    @Test
    public void testMinimalPlainTopology() {
        TestHelper.runTopology(auraClient, PlainTopologyExamples.two_layer_point2point_small(auraClient, executionUnits));
    }

    @Test
    public void testExtendedTopology() {
        TestHelper.runTopology(auraClient, PlainTopologyExamples.six_layer_all2all(auraClient, executionUnits));
    }

    @Test
    public void testPlainTopologiesConcurrently() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();
        topologies.add(PlainTopologyExamples.two_layer_point2point_small(auraClient, executionUnits / 2));
        topologies.add(PlainTopologyExamples.two_layer_point2point_small(auraClient, executionUnits / 2));
        TestHelper.runTopologiesConcurrently(auraClient, topologies);
    }

    @Test
    public void testMultipleQueriesSequentially() {
        List<Topology.AuraTopology> topologies = new ArrayList<>();

        // 2 layered - all2all connection
        topologies.add(PlainTopologyExamples.two_layer_point2point_small(auraClient, executionUnits));

        // 3 layered - point2point + point2point connection
        topologies.add(PlainTopologyExamples.three_layer_point2point(auraClient, executionUnits));

        // 3 layered - all2all + point2point connection
        topologies.add(PlainTopologyExamples.three_layer_all2all_point2point(auraClient, executionUnits));

        // 3 layered - point2point + all2all connection
        topologies.add(PlainTopologyExamples.three_layer_point2point_all2all(auraClient, executionUnits));

        // 3 layered - all2all + all2all connection
        topologies.add(PlainTopologyExamples.three_layer_all2all_all2all(auraClient, executionUnits));

        // 3 layered - point2point (join) point2point connection
        topologies.add(PlainTopologyExamples.three_layer_point2point_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) point2point connection
        topologies.add(PlainTopologyExamples.three_layer_all2all_join_point2point(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection
        topologies.add(PlainTopologyExamples.three_layer_all2all_join_all2all(auraClient, executionUnits));

        // 3 layered - all2all (join) all2all connection (small/large)
        topologies.add(PlainTopologyExamples.three_layer_all2all_join_all2all_sl(auraClient, executionUnits));

        TestHelper.runTopologies(auraClient, topologies);
    }

    @Test
    public void testOperatorTopology1() {
        final Topology.AuraTopology topology1 = OperatorTopologyExamples.testJob1(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology1);
    }

    @Test
    public void testOperatorTopology2() {
        final Topology.AuraTopology topology2 = OperatorTopologyExamples.testJob2(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology2);
    }

    @Test
    public void testOperatorTopology3() {
        final Topology.AuraTopology topology3 = OperatorTopologyExamples.testJob3(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology3);
    }

    @Test
    public void testOperatorTopology4() {
        final Topology.AuraTopology topology4 = OperatorTopologyExamples.testJob4(auraClient, executionUnits);
        TestHelper.runTopology(auraClient, topology4);
    }

    @AfterClass
    public static void tearDown() {

        if (clusterSimulator != null) {
            clusterSimulator.shutdown();
        }

        auraClient.closeSession();
    }

}
