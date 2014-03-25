package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.*;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;
import org.apache.log4j.Logger;

public class TopologyDeployer extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TopologyDeployer.class);

    private final RPCManager rpcManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param rpcManager
     */
    public TopologyDeployer(final RPCManager rpcManager) {
        // sanity check.
        if (rpcManager == null)
            throw new IllegalArgumentException("rpcManager == null");

        this.rpcManager = rpcManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public AuraTopology apply(AuraTopology topology) {

        deployTopology(topology);

        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY));

        return topology;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param topology
     */
    private synchronized void deployTopology(final AuraTopology topology) {

        // Deploying.
        TopologyBreadthFirstTraverser.traverseBackwards(topology, new Visitor<Node>() {

            @Override
            public void visit(final Node element) {
                for (final ExecutionNode en : element.getExecutionNodes()) {
                    final TaskDeploymentDescriptor tdd =
                            new TaskDeploymentDescriptor(en.getTaskDescriptor(),
                                    en.getTaskBindingDescriptor(),
                                    en.logicalNode.dataPersistenceType,
                                    en.logicalNode.executionType);
                    final WM2TMProtocol tmProtocol =
                            rpcManager.getRPCProtocolProxy(WM2TMProtocol.class, en.getTaskDescriptor().getMachineDescriptor());
                    tmProtocol.installTask(tdd);
                    LOG.info("TASK DEPLOYMENT DESCRIPTOR [" + en.getTaskDescriptor().name + "]: " + tdd.toString());
                }
            }
        });
    }
}
