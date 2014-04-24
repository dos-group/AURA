package de.tuberlin.aura.core.protocols;

import java.util.UUID;

import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;

public interface ClientWMProtocol {

    public void openSession(final UUID sessionID);

    public void submitTopology(final UUID sessionID, final AuraTopology topology);

    public void closeSession(final UUID sessionID);
}
