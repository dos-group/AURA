package de.tuberlin.aura.workloadmanager;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.workloadmanager.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.workloadmanager.TopologyStateMachine.TopologyTransition;

public class TopologyDeployer extends AssemblyPhase<AuraTopology, AuraTopology> {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public TopologyDeployer(final RPCManager rpcManager) {
		// sanity check.
		if (rpcManager == null)
			throw new IllegalArgumentException("rpcManager == null");

		this.rpcManager = rpcManager;
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(TopologyDeployer.class);

	private final RPCManager rpcManager;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	@Override
	public AuraTopology apply(AuraTopology topology) {

		deployTopology(topology);

		dispatcher.dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY));

		return topology;
	}

	// ---------------------------------------------------
	// Private.
	// ---------------------------------------------------

	private synchronized void deployTopology(final AuraTopology topology) {

		// Deploying.
		TopologyBreadthFirstTraverser.traverseBackwards(topology, new Visitor<Node>() {

			@Override
			public void visit(final Node element) {
				for (final ExecutionNode en : element.getExecutionNodes()) {
					final TaskDeploymentDescriptor tdd =
						new TaskDeploymentDescriptor(en.getTaskDescriptor(),
							en.getTaskBindingDescriptor(), en.logicalNode.dataPersistenceType, en.logicalNode.executionType);
					final WM2TMProtocol tmProtocol =
						rpcManager.getRPCProtocolProxy(WM2TMProtocol.class,
							en.getTaskDescriptor().getMachineDescriptor());
					tmProtocol.installTask(tdd);
					LOG.info("TASK DEPLOYMENT DESCRIPTOR [" + en.getTaskDescriptor().name + "]: " + tdd.toString());
				}
			}
		});
	}
}
