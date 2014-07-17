package de.tuberlin.aura.workloadmanager;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.common.utils.Visitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.DeploymentDescriptor;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.Node;
import de.tuberlin.aura.core.topology.Topology.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

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

                    final WM2TMProtocol tmProtocol =
                            rpcManager.getRPCProtocolProxy(WM2TMProtocol.class, en.getNodeDescriptor().getMachineDescriptor());

                    if (!en.logicalNode.isAlreadyDeployed) {

                        final DeploymentDescriptor tdd =
                                new Descriptors.DeploymentDescriptor(en.getNodeDescriptor(),
                                                                     en.getNodeBindingDescriptor(),
                                                                     en.logicalNode.dataPersistenceType,
                                                                     en.logicalNode.executionType);

                        tmProtocol.installTask(tdd);

                        LOG.debug("TASK DEPLOYMENT DESCRIPTOR [" + en.getNodeDescriptor().name + "]: " + tdd.toString());

                    } else {

                        tmProtocol.addOutputBinding(en.getNodeDescriptor().taskID, en.getNodeBindingDescriptor().outputGateBindings);
                    }
                }
            }
        });

        /*
         * final Map<Descriptors.MachineDescriptor, List<DeploymentDescriptor>> machineDeployment =
         * new HashMap<Descriptors.MachineDescriptor, List<DeploymentDescriptor>>();
         * 
         * // Collect all deployment descriptors for a machine.
         * TopologyBreadthFirstTraverser.traverseBackwards(topology, new Visitor<Node>() {
         * 
         * @Override public void visit(final Node element) { for (final ExecutionNode en :
         * element.getExecutionNodes()) {
         * 
         * final DeploymentDescriptor tdd = new DeploymentDescriptor(en.getNodeDescriptor(),
         * en.getBindingDescriptor(), en.logicalNode.dataPersistenceType,
         * en.logicalNode.executionType);
         * 
         * final Descriptors.MachineDescriptor machineDescriptor =
         * en.getNodeDescriptor().getMachineDescriptor();
         * 
         * List<DeploymentDescriptor> deploymentDescriptors =
         * machineDeployment.get(machineDescriptor);
         * 
         * if (deploymentDescriptors == null) { deploymentDescriptors = new
         * ArrayList<DeploymentDescriptor>(); machineDeployment.put(machineDescriptor,
         * deploymentDescriptors); }
         * 
         * deploymentDescriptors.add(tdd); } } });
         * 
         * // Ship the deployment descriptors to the task managers.
         * TopologyBreadthFirstTraverser.traverseBackwards(topology, new Visitor<Node>() {
         * 
         * @Override public void visit(final Node element) { for (final ExecutionNode en :
         * element.getExecutionNodes()) {
         * 
         * final Descriptors.MachineDescriptor machineDescriptor =
         * en.getNodeDescriptor().getMachineDescriptor();
         * 
         * List<DeploymentDescriptor> tddList = machineDeployment.get(machineDescriptor);
         * 
         * // If TDD are not yet shipped, then do it... if (tddList != null) {
         * 
         * final WM2TMProtocol tmProtocol = rpcManager.getRPCProtocolProxy(WM2TMProtocol.class,
         * machineDescriptor);
         * 
         * tmProtocol.installTasks(tddList);
         * 
         * // ... and remove it from our mapping. machineDeployment.remove(machineDescriptor); } } }
         * });
         */
    }
}
