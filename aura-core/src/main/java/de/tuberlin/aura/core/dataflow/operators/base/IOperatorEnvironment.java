package de.tuberlin.aura.core.dataflow.operators.base;

import org.slf4j.Logger;

import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;

/**
 *
 */
public interface IOperatorEnvironment {

    public abstract Logger getLogger();

    public abstract DataflowNodeProperties getProperties();
}
