package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFunction;


public abstract class AbstractFunction implements IFunction {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private IExecutionContext environment;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create() {
    }

    public IExecutionContext getEnvironment() {
        return environment;
    }

    public void setEnvironment(final IExecutionContext environment) {
        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("context == null");

        this.environment = environment;
    }

    public void release() {
    }
}
