package de.tuberlin.aura.core.protocols;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.taskmanager.TaskManagerStatus;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;

public interface IClientWMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void openSession(final UUID sessionID);

    public abstract void submitTopology(final UUID sessionID, final AuraTopology topology);

    public abstract void closeSession(final UUID sessionID);


    public abstract <E> Collection<E> gatherDataset(final UUID uid);

    public abstract <E> void scatterDataset(final UUID datasetID, final Collection<E> dataset);

    public abstract void eraseDataset(final UUID datasetID);

    public abstract void assignDataset(final UUID dstDatasetID, final UUID srcDatasetID);


    public abstract List<TaskManagerStatus> getClusterUtilization();
}
