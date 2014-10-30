package de.tuberlin.aura.workloadmanager;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.Topology.ExecutionNode;
import de.tuberlin.aura.core.topology.Topology.LogicalNode;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;
import de.tuberlin.aura.core.filesystem.InputSplit;

import static de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import static de.tuberlin.aura.workloadmanager.LocationPreference.PreferenceLevel;


public class TopologyScheduler extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologyScheduler.class);

    private IInfrastructureManager infrastructureManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyScheduler(final IInfrastructureManager infrastructureManager, IConfig config) {
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

        scheduleTopology(topology);

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

        for (LogicalNode node : topology.nodesFromSourceToSink()) {
            if (node.hasCoLocationRequirements()) {
                nodesWithCoLocationRequirements.add(node);

                UUID taskToCoLocateTo = (UUID)node.propertiesList.get(0).config.get(AbstractPhysicalOperator.CO_LOCATION_TASKID);
                nodesRequiredToCoLocateTo.add(topology.nodeMap.get(taskToCoLocateTo));
            } else if (node.isHDFSSource()) {
                nodesWithPreferredLocations.add(node);
            }
        }

        // schedule nodes required by others first (e.g. datasets), then the nodes with requirements (e.g. dataset update ops)

        scheduleCollectionOfElements(nodesRequiredToCoLocateTo, topology);
        scheduleCollectionOfElements(nodesWithCoLocationRequirements, topology);

        // schedule nodes that prefer locations (e.g. HDFSSources)

        scheduleCollectionOfElements(nodesWithPreferredLocations, topology);

        // schedule all remaining nodes

        List<LogicalNode> remainingNodes = new ArrayList<>(topology.nodesFromSourceToSink());
        remainingNodes.removeAll(nodesRequiredToCoLocateTo);
        remainingNodes.removeAll(nodesWithCoLocationRequirements);
        remainingNodes.removeAll(nodesWithPreferredLocations);

        scheduleCollectionOfElements(remainingNodes, topology);
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

                MachineDescriptor machine;

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

            UUID taskToCoLocateTo = (UUID)element.propertiesList.get(0).config.get(AbstractPhysicalOperator.CO_LOCATION_TASKID);
            final LogicalNode taskToColocateTo = topology.nodeMap.get(taskToCoLocateTo);

            if (taskToColocateTo == null)
                throw new IllegalStateException("Task to co-locate to not found.");

            if (!taskToColocateTo.isAlreadyDeployed)
                throw new IllegalStateException("Task to co-locate to not yet deployed.");

            for (ExecutionNode executionNode : taskToColocateTo.getExecutionNodes()) {
                MachineDescriptor machine = executionNode.getNodeDescriptor().getMachineDescriptor();
                locationPreferences.add(new LocationPreference(machine, PreferenceLevel.REQUIRED));
            }

        } else if (element.isHDFSSource()) {

            List<InputSplit> inputSplits = infrastructureManager.registerHDFSSource(element);

            // shuffle in case HDFS returns the inputSplits ordered / with similar orders
            // to in the end assign inputSplits roughly evenly to hosts
            Collections.shuffle(inputSplits);

            locationPreferences = new LinkedList<>();

            for (InputSplit inputSplit : inputSplits) {
                locationPreferences.add(new LocationPreference(infrastructureManager.getMachinesWithInputSplit(inputSplit), PreferenceLevel.PREFERRED));
            }

        }
        return locationPreferences;
    }

}
