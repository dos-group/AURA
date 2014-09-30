package de.tuberlin.aura.core.dataflow.operators.base;

import java.io.Serializable;

import org.slf4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;


/**
 *
 */
public interface IOperatorEnvironment extends Serializable {

    public abstract Logger getLogger();

    public abstract DataflowNodeProperties getProperties();

    public abstract Descriptors.OperatorNodeDescriptor getNodeDescriptor();
}
