package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFunction;

/**
 *
 */
public abstract class AbstractFunction implements IFunction {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private IOperatorEnvironment environment;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create() {
    }

    public IOperatorEnvironment getEnvironment() {
        return environment;
    }

    public void setEnvironment(final IOperatorEnvironment environment) {
        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("environment == null");

        this.environment = environment;
    }

    public void release() {
    }
}
