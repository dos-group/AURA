package de.tuberlin.aura.core.dataflow.udfs.contracts;

import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;

/**
 *
 */
public interface IFunction {

    public void create();

    public IOperatorEnvironment getEnvironment();

    public void setEnvironment(final IOperatorEnvironment environment);

    public void release();
}
