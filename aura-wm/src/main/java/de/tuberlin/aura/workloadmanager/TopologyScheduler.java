package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.topology.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyTransition;

public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

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
