package de.tuberlin.aura.core.protocols;

import java.util.UUID;

import de.tuberlin.aura.core.topology.Topology.AuraTopology;

public interface IClientWMProtocol {

    public abstract void openSession(final UUID sessionID);

    public abstract void submitTopology(final UUID sessionID, final AuraTopology topology);

    public abstract void closeSession(final UUID sessionID);
}
