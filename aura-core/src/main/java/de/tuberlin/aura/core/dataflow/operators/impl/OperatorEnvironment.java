package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.descriptors.Descriptors;
import org.slf4j.Logger;

import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;

/**
 *
 */
public class OperatorEnvironment implements IOperatorEnvironment {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Logger logger;

    private final DataflowNodeProperties properties;

    private final Descriptors.OperatorNodeDescriptor descriptor;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public OperatorEnvironment(final Logger logger,
                               final DataflowNodeProperties properties,
                               final Descriptors.OperatorNodeDescriptor descriptor) {
        // sanity check.
        if (logger == null)
            throw new IllegalArgumentException("logger == null");
        if (properties == null)
            throw new IllegalArgumentException("properties == null");
        if (descriptor == null)
            throw new IllegalArgumentException("descriptor == null");

        this.logger = logger;

        this.properties = properties;

        this.descriptor = descriptor;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public DataflowNodeProperties getProperties() {
        return properties;
    }

    @Override
    public Descriptors.OperatorNodeDescriptor getNodeDescriptor() {
        return descriptor;
    }
}
