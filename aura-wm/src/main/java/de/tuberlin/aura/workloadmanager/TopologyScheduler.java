package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.*;
import de.tuberlin.aura.core.topology.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyTransition;
import org.apache.log4j.Logger;

public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

    private static final Logger LOG = Logger.getLogger(TopologyScheduler.class);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyScheduler(final InfrastructureManager infrastructureManager) {
        // sanity check.
        if (infrastructureManager == null)
            throw new IllegalArgumentException("infrastructureManager == null");

        this.infrastructureManager = infrastructureManager;
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private InfrastructureManager infrastructureManager;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public AuraTopology apply(AuraTopology topology) {

        scheduleTopology(topology);

        for (ExecutionNode node : topology.executionNodeMap.values()) {

            LOG.info(node + " " + node.getTaskBindingDescriptor().task.getMachineDescriptor());
        }

        dispatcher.dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));

        return topology;
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private void scheduleTopology(AuraTopology topology) {

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

            @Override
            public void visit(final Node element) {
                for (final ExecutionNode en : element.getExecutionNodes()) {
                    en.getTaskDescriptor().setMachineDescriptor(infrastructureManager.getNextMachine());
                }
            }
        });
    }
}
