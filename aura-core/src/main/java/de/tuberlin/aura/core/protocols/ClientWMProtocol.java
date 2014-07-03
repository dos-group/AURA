package de.tuberlin.aura.core.protocols;

import java.util.UUID;

import de.tuberlin.aura.core.topology.Topology.AuraTopology;

public interface ClientWMProtocol {

    public abstract void openSession(final UUID sessionID);

    public abstract void submitTopology(final UUID sessionID, final AuraTopology topology);

    public abstract void closeSession(final UUID sessionID);


    public abstract void submitToTopology(final UUID sessionID, final UUID topologyID, final AuraTopology topology);

    //public abstract void connectTopologies(final UUID sessionID, final UUID topologyID1, final UUID taskNodeID1, final UUID topologyID2, final UUID taskNodeID2);
}
