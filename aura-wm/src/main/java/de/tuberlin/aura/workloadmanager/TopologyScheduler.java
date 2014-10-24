package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;
import de.tuberlin.aura.core.topology.Topology.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;
import de.tuberlin.aura.workloadmanager.spi.IDistributedEnvironment;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologyScheduler.class);

    private IInfrastructureManager infrastructureManager;

    private IDistributedEnvironment environment;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyScheduler(final IInfrastructureManager infrastructureManager,
                             final IDistributedEnvironment environment) {
        // sanity check.
        if (infrastructureManager == null)
            throw new IllegalArgumentException("infrastructureManager == null");
        if (environment == null)
            throw new IllegalArgumentException("context == null");

        this.infrastructureManager = infrastructureManager;

        this.environment = environment;
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

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<LogicalNode>() {

            @Override
            public void visit(final LogicalNode element) {

                if (element.propertiesList.get(0).type == DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET
                        && element.propertiesList.get(0).coLocatedDatasetID != null) {

                    final UUID coLocatedDatasetID = element.propertiesList.get(0).coLocatedDatasetID;
                    final LogicalNode coLocatedDataset = environment.getDataset(coLocatedDatasetID);

                    if (coLocatedDataset == null)
                        throw new IllegalStateException("primary dataset not found");

                    for (int i = 0; i <  coLocatedDataset.getExecutionNodes().size(); ++i) {
                        final ExecutionNode coLocatedEN = coLocatedDataset.getExecutionNodes().get(i);
                        element.getExecutionNodes().get(i).getNodeDescriptor().setMachineDescriptor(coLocatedEN.getNodeDescriptor().getMachineDescriptor());
                    }

                } else {

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
            }
        });
    }
}
