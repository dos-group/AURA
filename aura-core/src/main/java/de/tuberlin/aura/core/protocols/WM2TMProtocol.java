package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;

public interface WM2TMProtocol {

	public void installTask( final TaskDescriptor taskDescriptor, 
							 final TaskBindingDescriptor taskBindingDescriptor );
		
}
