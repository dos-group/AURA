package de.tuberlin.aura.tests.integration;

import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Suite;
import org.junit.runner.RunWith;

import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        PlainTopologiesTest.class,
        DataflowTest.class,
        ParallelDataflowTest.class,
        DistributedEnvironmentTest.class,
        IterativeDataflowTest.class})

public class IntegrationTestSuite {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestSuite.class);

    public static LocalClusterSimulator clusterSimulator = null;

    public static boolean isRunning = false;

    // --------------------------------------------------
    // Test Suite Methods.
    // --------------------------------------------------

    @BeforeClass
    public static void setIsRunningTrue() {
        isRunning = true;
    }

    @BeforeClass
    public static void setUpTestEnvironment() {

        // clusterSimulator is started and stopped for all tests just once since starting and stopping per test in each
        // test's setUp and tearDown leads to concurrent tearDown/setUp-issues when tests are executed from the suite.

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
    }

    @AfterClass
    public static void setIsRunningFalse() {
        isRunning = false;
    }

    @AfterClass
    public static void tearDownTestEnvironment() {

        if (clusterSimulator != null) {
            clusterSimulator.shutdown();
            clusterSimulator = null;
        }

    }

}