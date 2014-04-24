package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;

import java.util.List;

public interface WM2TMProtocol {

    public void installTask(final TaskDeploymentDescriptor taskDeploymentDescriptor);

    public void installTasks(final List<TaskDeploymentDescriptor> deploymentDescriptors);
}
