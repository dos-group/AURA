package de.tuberlin.aura.core.protocols;

import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;

public interface WM2TMProtocol {

    public abstract void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor);

    public abstract void installTasks(final List<TaskDeploymentDescriptor> deploymentDescriptors);

    public abstract void addOutputBinding(final UUID taskID, final List<List<Descriptors.TaskDescriptor>> outputBinding);
}
