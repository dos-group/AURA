package de.tuberlin.aura.workloadmanager;

import java.util.*;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


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

        LOG.info("ENTER SCHEDULE");

        scheduleTopology(topology);

        LOG.info("LEAVE SCHEDULE");


        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE));

        return topology;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void scheduleTopology(final AuraTopology topology) {
        LOG.debug("Schedule topology [{}] on {} taskmanager managers", topology.name, infrastructureManager.getNumberOfMachines());

        List<LogicalNode> nodesRequiredToCoLocateTo = new ArrayList<>();
        List<LogicalNode> nodesWithCoLocationRequirements = new ArrayList<>();
        List<LogicalNode> nodesWithPreferredLocations = new ArrayList<>();
        List<LogicalNode> unconstrainedNodes = new ArrayList<>(topology.nodesFromSourceToSink());

        for (LogicalNode node : topology.nodesFromSourceToSink()) {
            if (node.hasCoLocationRequirements()) {
                nodesWithCoLocationRequirements.add(node);

                String taskToCoLocateToName = (String)node.propertiesList.get(0).config.get(AbstractPhysicalOperator.CO_LOCATION_TASK_NAME);
                nodesRequiredToCoLocateTo.add(topology.nodeMap.get(taskToCoLocateToName));
            } else if (node.isHDFSSource()) {
                nodesWithPreferredLocations.add(node);
            }
        }

        unconstrainedNodes.removeAll(nodesRequiredToCoLocateTo);
        unconstrainedNodes.removeAll(nodesWithCoLocationRequirements);
        unconstrainedNodes.removeAll(nodesWithPreferredLocations);

        scheduleCollectionOfElements(nodesRequiredToCoLocateTo, topology);
        scheduleCollectionOfElements(nodesWithCoLocationRequirements, topology);
        scheduleCollectionOfElements(nodesWithPreferredLocations, topology);
        scheduleCollectionOfElements(unconstrainedNodes, topology);
    }

    private void scheduleCollectionOfElements(final Collection<LogicalNode> nodes, AuraTopology topology) {
        for (LogicalNode node : nodes) {
            scheduleElement(node, topology);
        }
    }

    private void scheduleElement(final LogicalNode element, final AuraTopology topology) {

        Queue<LocationPreference> locationPreferences = computeLocationPreferences(element, topology);

        for (final ExecutionNode en : element.getExecutionNodes()) {
            if (!en.logicalNode.isAlreadyDeployed) {

                Descriptors.MachineDescriptor machine;

                if (locationPreferences != null && !locationPreferences.isEmpty()) {
                    machine = infrastructureManager.getMachine(locationPreferences.poll());
                } else {
                    machine = infrastructureManager.getMachine(null);
                }

                en.getNodeDescriptor().setMachineDescriptor(machine);
            }
            LOG.debug(en.getNodeDescriptor().getMachineDescriptor().address.toString()
                    + " -> " + en.getNodeDescriptor().name + "_"
                    + en.getNodeDescriptor().taskIndex);
        }
    }

    private Queue<LocationPreference> computeLocationPreferences(LogicalNode element, AuraTopology topology) {
        Queue<LocationPreference> locationPreferences = new LinkedList<>();

        if (element.hasCoLocationRequirements()) {

            String taskToCoLocateToName = (String)element.propertiesList.get(0).config.get(AbstractPhysicalOperator.CO_LOCATION_TASK_NAME);
            final LogicalNode taskToCoLocateTo = topology.nodeMap.get(taskToCoLocateToName);

            if (taskToCoLocateTo == null)
                throw new IllegalStateException("Task to co-locate to not found.");

            if (!taskToCoLocateTo.isAlreadyDeployed)
                throw new IllegalStateException("Task to co-locate to not yet deployed.");

            for (ExecutionNode executionNode : taskToCoLocateTo.getExecutionNodes()) {
                Descriptors.MachineDescriptor machine = executionNode.getNodeDescriptor().getMachineDescriptor();
                locationPreferences.add(new LocationPreference(machine, LocationPreference.PreferenceLevel.REQUIRED));
            }

        } else if (element.isHDFSSource()) {

            List<InputSplit> inputSplits = infrastructureManager.registerHDFSSource(element);

            locationPreferences = new LinkedList<>();

            for (InputSplit inputSplit : inputSplits) {
                locationPreferences.add(new LocationPreference(infrastructureManager.getMachinesWithInputSplit(inputSplit), LocationPreference.PreferenceLevel.PREFERRED));
            }

        }
        return locationPreferences;
    }
}