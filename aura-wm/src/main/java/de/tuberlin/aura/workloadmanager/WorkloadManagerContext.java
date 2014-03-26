package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;

public final class WorkloadManagerContext {

    public final WorkloadManager workloadManager;

    public final IOManager ioManager;

    public final RPCManager rpcManager;

    public final InfrastructureManager infrastructureManager;

    public WorkloadManagerContext(final WorkloadManager workloadManager,
                                  final IOManager ioManager,
                                  final RPCManager rpcManager,
                                  final InfrastructureManager infrastructureManager) {

        this.workloadManager = workloadManager;

        this.ioManager = ioManager;

        this.rpcManager = rpcManager;

        this.infrastructureManager = infrastructureManager;
    }
}
