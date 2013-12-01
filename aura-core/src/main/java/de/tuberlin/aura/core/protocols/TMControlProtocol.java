package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.task.TaskUserCode;

public interface TMControlProtocol {

	public void installTask( final TaskDescriptor taskDescriptor, final TaskBindingDescriptor taskBindingDescriptor, 
			final TaskUserCode taskUserCode );
		
}
