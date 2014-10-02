package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.iosystem.spi.IRPCManager;
import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.DeploymentDescriptor;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.IWM2TMProtocol;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;
import de.tuberlin.aura.core.topology.Topology.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public class TopologyDeployer extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TopologyDeployer.class);

    private final IRPCManager rpcManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyDeployer(final IRPCManager rpcManager) {
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

        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY));

        deployTopology(topology);

        return topology;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private synchronized void deployTopology(final AuraTopology topology) {

        // Deploying.
        TopologyBreadthFirstTraverser.traverseBackwards(topology, new IVisitor<LogicalNode>() {

            @Override
            public void visit(final LogicalNode element) {
                for (final ExecutionNode en : element.getExecutionNodes()) {

                    final IWM2TMProtocol tmProtocol =
                            rpcManager.getRPCProtocolProxy(IWM2TMProtocol.class, en.getNodeDescriptor().getMachineDescriptor());

                    if(!en.logicalNode.isAlreadyDeployed) {

                        final DeploymentDescriptor tdd =
                                new Descriptors.DeploymentDescriptor(
                                        en.getNodeDescriptor(),
                                        en.getNodeBindingDescriptor(),
                                        en.logicalNode.dataPersistenceType,
                                        en.logicalNode.executionType
                                );

                        tmProtocol.installTask(tdd);

                        LOG.debug("TASK DEPLOYMENT DESCRIPTOR [" + en.getNodeDescriptor().name + "]: " + tdd.toString());

                    } else {

                        if (en.getNodeDescriptor() instanceof Descriptors.DatasetNodeDescriptor) {

                            final Descriptors.DatasetNodeDescriptor datasetNodeDescriptor = (Descriptors.DatasetNodeDescriptor) en.getNodeDescriptor();

                            tmProtocol.addOutputBinding(
                                    en.getNodeDescriptor().taskID,
                                    topology.topologyID,
                                    en.getNodeBindingDescriptor().outputGateBindings,
                                    datasetNodeDescriptor.properties.strategy,
                                    datasetNodeDescriptor.properties.partitioningKeys
                             );

                        } else {
                            throw new IllegalStateException("Dynamic Binding only for Datasets supported.");
                        }
                    }
                }
            }
        });
    }
}
