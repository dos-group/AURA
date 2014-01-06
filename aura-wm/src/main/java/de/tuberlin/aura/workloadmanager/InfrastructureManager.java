package de.tuberlin.aura.workloadmanager;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.workloadmanager.spi.IInfrastructureManager;

/**
 * Dummy Implementation.
 */
public class InfrastructureManager implements IInfrastructureManager {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public InfrastructureManager() {

		this.workerMachines = new ArrayList<MachineDescriptor>();

		workerMachines.add(LocalDeployment.MACHINE_1_DESCRIPTOR);
		workerMachines.add(LocalDeployment.MACHINE_2_DESCRIPTOR);
		workerMachines.add(LocalDeployment.MACHINE_3_DESCRIPTOR);
		workerMachines.add(LocalDeployment.MACHINE_4_DESCRIPTOR);

		machineIdx = 0;
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private final List<MachineDescriptor> workerMachines;

	private int machineIdx;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public synchronized MachineDescriptor getMachine() {
		final MachineDescriptor md = workerMachines.get(machineIdx);
		machineIdx = (++machineIdx % workerMachines.size());
		return md;
	}
}
