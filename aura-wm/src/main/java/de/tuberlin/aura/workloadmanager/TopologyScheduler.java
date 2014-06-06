package de.tuberlin.aura.workloadmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.topology.AuraGraph.*;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(TopologyScheduler.class);

    private InfrastructureManager infrastructureManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param infrastructureManager
     */
    public TopologyScheduler(final InfrastructureManager infrastructureManager) {
        // sanity check.
        if (infrastructureManager == null)
            throw new IllegalArgumentException("infrastructureManager == null");

        this.infrastructureManager = infrastructureManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param topology
     * @return
     */
    @Override
    public AuraTopology apply(AuraTopology topology) {

        scheduleTopology(topology);

        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));

        return topology;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param topology
     */
    private void scheduleTopology(AuraTopology topology) {
        LOG.debug("Schedule topology [{}] on {} task managers", topology.name, infrastructureManager.getNumberOfMachine());

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

            @Override
            public void visit(final Node element) {

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
