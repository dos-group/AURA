package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSourcePhysicalOperator;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.filesystem.InputSplitAssigner;
import de.tuberlin.aura.core.filesystem.LocatableInputSplitAssigner;
import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.filesystem.in.InputFormat;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;
import de.tuberlin.aura.core.topology.Topology.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologyScheduler.class);

    private IInfrastructureManager infrastructureManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyScheduler(final IInfrastructureManager infrastructureManager) {
        // sanity check.
        if (infrastructureManager == null)
            throw new IllegalArgumentException("infrastructureManager == null");

        this.infrastructureManager = infrastructureManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public AuraTopology apply(AuraTopology topology) {

        scheduleTopology(topology);

        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));

        return topology;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void scheduleTopology(AuraTopology topology) {
        LOG.debug("Schedule topology [{}] on {} taskmanager managers", topology.name, infrastructureManager.getNumberOfMachine());

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<LogicalNode>() {

            @Override
            public void visit(final LogicalNode element) {

                if (element.propertiesList.get(0).type == DataflowNodeProperties.DataflowNodeType.HDFS_SOURCE) {
                    infrastructureManager.registerHDFSSource(element);
                }

                for (final ExecutionNode en : element.getExecutionNodes()) {
                    if (!en.logicalNode.isAlreadyDeployed) {
                        en.getNodeDescriptor().setMachineDescriptor(infrastructureManager.getNextMachine());
                    }
                    LOG.debug(en.getNodeDescriptor().getMachineDescriptor().address.toString()
                            + " -> " + en.getNodeDescriptor().name + "_"
                            + en.getNodeDescriptor().taskIndex);
                }
            }
        });
    }

    public static class InputSplitManager implements Serializable {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final IWorkloadManager workloadManager;

        private Map<UUID, InputSplitAssigner> inputSplitMap;

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public InputSplitManager(final IWorkloadManager workloadManager) {
            // sanity check.
            if (workloadManager == null)
                throw new IllegalArgumentException("workloadManager == null");

            this.workloadManager = workloadManager;

            this.inputSplitMap = new ConcurrentHashMap<>();
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void registerHDFSSource(final LogicalNode node) {
            if (node == null)
                throw new IllegalArgumentException("node == null");
            if (node.propertiesList.get(0) == null)
                throw new IllegalStateException("properties == null");

            final Path path = new Path((String)node.propertiesList.get(0).config.get(HDFSSourcePhysicalOperator.HDFS_SOURCE_FILE_PATH));
            final Class<?>[] fieldTypes = (Class<?>[]) node.propertiesList.get(0).config.get(HDFSSourcePhysicalOperator.HDFS_SOURCE_INPUT_FIELD_TYPES);

            @SuppressWarnings("unchecked")
            final InputFormat inputFormat = new CSVInputFormat(path, fieldTypes);

            final Configuration conf = new Configuration();
            conf.set("fs.defaultFS", workloadManager.getConfig().getString("wm.io.hdfs.hdfs_url"));
            inputFormat.configure(conf);

            final InputSplit[] inputSplits;
            try {
                inputSplits = inputFormat.createInputSplits(node.propertiesList.get(0).globalDOP);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            final InputSplitAssigner inputSplitAssigner = new LocatableInputSplitAssigner((FileInputSplit[])inputSplits);
            inputSplitMap.put(node.uid, inputSplitAssigner);
        }

        public synchronized InputSplit getInputSplitFromHDFSSource(final ExecutionNode exNode) {
            if (exNode == null)
                throw new IllegalArgumentException("exNode == null");
            final InputSplitAssigner inputSplitAssigner = inputSplitMap.get(exNode.logicalNode.uid);
            return inputSplitAssigner.getNextInputSplit(exNode);
        }
    }
}
