package de.tuberlin.aura.core.protocols;

import java.util.List;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;

public interface WM2TMProtocol {

    public void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor);

    public void installTasks(final List<TaskDeploymentDescriptor> deploymentDescriptors);
}
