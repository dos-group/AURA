package de.tuberlin.aura.core.protocols;

import java.util.List;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.task.usercode.UserCode;

public interface ClientWMProtocol {
	
	public void submitProgram( final List<UserCode> userCode, 
							   final List<TaskDescriptor> tasks, 
							   final List<TaskBindingDescriptor> bindings );
	
}
