package de.tuberlin.aura.core.dataflow.operators.impl;

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

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public OperatorEnvironment(final Logger logger,
                               final DataflowNodeProperties properties) {
        // sanity check.
        if (logger == null)
            throw new IllegalArgumentException("logger == null");
        if (properties == null)
            throw new IllegalArgumentException("properties == null");

        this.logger = logger;

        this.properties = properties;
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
}
