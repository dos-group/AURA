package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;

import java.util.UUID;

public interface ClientWMProtocol {

    public void openSession(final UUID sessionID);

    public void submitTopology(final UUID sessionID, final AuraTopology topology);

    public void closeSession(final UUID sessionID);
}
