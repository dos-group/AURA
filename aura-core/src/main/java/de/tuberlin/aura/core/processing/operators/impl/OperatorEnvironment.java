package de.tuberlin.aura.core.processing.operators.impl;

import org.slf4j.Logger;

import de.tuberlin.aura.core.processing.api.OperatorProperties;
import de.tuberlin.aura.core.processing.operators.base.IOperatorEnvironment;

/**
 *
 */
public class OperatorEnvironment implements IOperatorEnvironment {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Logger logger;

    private final OperatorProperties properties;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public OperatorEnvironment(final Logger logger,
                               final OperatorProperties properties) {
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
    public OperatorProperties getProperties() {
        return properties;
    }
}
