package de.tuberlin.aura.core.processing.udfs.contracts;

import de.tuberlin.aura.core.processing.operators.base.IOperatorEnvironment;

/**
 *
 */
public interface IFunction {

    public void create();

    public IOperatorEnvironment getEnvironment();

    public void setEnvironment(final IOperatorEnvironment environment);

    public void release();
}
