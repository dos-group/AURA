package de.tuberlin.aura.workloadmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.*;
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

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

            @Override
            public void visit(final Node element) {
                for (final ExecutionNode en : element.getExecutionNodes()) {
                    en.getTaskDescriptor().setMachineDescriptor(infrastructureManager.getNextMachine());

                    LOG.debug(en.getTaskDescriptor().getMachineDescriptor().address.toString() + " -> " + en.getTaskDescriptor().name + "_"
                            + en.getTaskDescriptor().taskIndex);
                }
            }
        });
    }
}
