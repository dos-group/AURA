package de.tuberlin.aura.client.executors;

import de.tuberlin.aura.core.common.utils.ProcessExecutor;
import de.tuberlin.aura.core.descriptors.Descriptors.HDDDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.HardwareDescriptor;
import de.tuberlin.aura.core.statistic.MeasurementManager;
import de.tuberlin.aura.core.zookeeper.ZkHelper;
import de.tuberlin.aura.taskmanager.TaskManager;
import de.tuberlin.aura.workloadmanager.WorkloadManager;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class LocalClusterExecutor {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final HardwareDescriptor MACHINE_HARDWARE = new HardwareDescriptor((short) 4,
            2L * 1024L * 1024L * 1024L,
            new HDDDescriptor(10L * 1024L * 1024L * 1024L));

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static enum LocalExecutionMode {

        EXECUTION_MODE_SINGLE_PROCESS,

        EXECUTION_MODE_MULTIPLE_PROCESSES
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalClusterExecutor(final LocalExecutionMode mode, boolean startupZookeeper, final String zkServer, int numNodes, String measurementPath) {
        this(mode, startupZookeeper, zkServer, numNodes, 2181, 5000, 2000, measurementPath);
    }

    public LocalClusterExecutor(final LocalExecutionMode mode,
                                boolean startupZookeeper,
                                final String zkServer,
                                int numNodes,
                                int zkClientPort,
                                int numConnections,
                                int tickTime,
                                String measurementPath) {
        // sanity check.
        ZkHelper.checkConnectionString(zkServer);
        if (numNodes < 1)
            throw new IllegalArgumentException("numNodes < 1");

        this.reservedPorts = new HashSet<Integer>();

        this.tmList = new ArrayList<TaskManager>();

        this.peList = new ArrayList<ProcessExecutor>();

        // ------- bootstrap zookeeper server -------

        if (startupZookeeper) {
            final File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile();
            if (dir.exists()) {
                try {
                    FileUtils.deleteDirectory(dir);
                } catch (IOException e) {
                    LOG.error(e);
                }
            }

            try {
                this.zookeeperServer = new ZooKeeperServer(dir, dir, tickTime);
                this.zookeeperServer.setMaxSessionTimeout(10000000);
                this.zookeeperCNXNFactory = new NIOServerCnxnFactory();
                this.zookeeperCNXNFactory.configure(new InetSocketAddress(zkClientPort), numConnections);
                this.zookeeperCNXNFactory.startup(zookeeperServer);
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        } else {
            zookeeperServer = null;
            zookeeperCNXNFactory = null;
        }

        // ------- bootstrap local cluster -------

        switch (mode) {

            case EXECUTION_MODE_SINGLE_PROCESS: {
                new WorkloadManager(zkServer, getFreePort(), getFreePort());

                MeasurementManager.setRoot(measurementPath);

                for (int i = 0; i < numNodes; ++i) {
                    tmList.add(new TaskManager(zkServer, getFreePort(), getFreePort()));
                }
            }
            break;

            case EXECUTION_MODE_MULTIPLE_PROCESSES: {
                try {
                    peList.add(new ProcessExecutor(WorkloadManager.class).execute(zkServer,
                            new Integer(getFreePort()).toString(),
                            new Integer(getFreePort()).toString(),
                            measurementPath));
                    Thread.sleep(1000);
                    for (int i = 0; i < numNodes; ++i) {
                        peList.add(new ProcessExecutor(TaskManager.class).execute(zkServer,
                                new Integer(getFreePort()).toString(),
                                new Integer(getFreePort()).toString(),
                                measurementPath));
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            break;

            default:
                throw new IllegalStateException("execution mode not known");
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(LocalClusterExecutor.class);

    private final Set<Integer> reservedPorts;

    private final List<TaskManager> tmList;

    private final List<ProcessExecutor> peList;

    private final ZooKeeperServer zookeeperServer;

    private final NIOServerCnxnFactory zookeeperCNXNFactory;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void shutdown() {
        for (final ProcessExecutor pe : peList) {
            pe.destroy();
        }
        this.zookeeperCNXNFactory.closeAll();
        System.exit(0);
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private int getFreePort() {
        int freePort = -1;
        do {
            try {
                final ServerSocket ss = new ServerSocket(0);
                freePort = ss.getLocalPort();
                ss.close();
            } catch (IOException e) {
            }
        } while (reservedPorts.contains(freePort) || freePort < 1024 || freePort > 65535);
        reservedPorts.add(freePort);
        return freePort;
    }
}
