package de.tuberlin.aura.core.dataflow.udfs.contracts;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.io.Serializable;


public interface IFunction extends Serializable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create();

    public IExecutionContext getEnvironment();

    public void setEnvironment(final IExecutionContext environment);

    public void release();
}
