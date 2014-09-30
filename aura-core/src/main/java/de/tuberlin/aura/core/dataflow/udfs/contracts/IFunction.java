package de.tuberlin.aura.core.dataflow.udfs.contracts;

import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;

import java.io.Serializable;

/**
 *
 */
public interface IFunction extends Serializable {

    public void create();

    public IOperatorEnvironment getEnvironment();

    public void setEnvironment(final IOperatorEnvironment environment);

    public void release();
}
