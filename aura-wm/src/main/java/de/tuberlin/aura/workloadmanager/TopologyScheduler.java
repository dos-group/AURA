package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
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

                if (element.properties.type == DataflowNodeProperties.DataflowNodeType.HDFS_SOURCE) {
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
}
