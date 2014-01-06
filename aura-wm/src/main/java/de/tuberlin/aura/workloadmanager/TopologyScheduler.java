package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.workloadmanager.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.workloadmanager.TopologyStateMachine.TopologyTransition;

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
					en.getTaskDescriptor().setMachineDescriptor(infrastructureManager.getMachine());
				}
			}
		});
	}
}
