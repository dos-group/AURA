package de.tuberlin.aura.core.dataflow.operators.base;

import java.io.Serializable;

import de.tuberlin.aura.core.common.utils.IVisitable;

/**
 *
 * @param <O>
 */
public interface IPhysicalOperator<O> extends Serializable, IVisitable<IPhysicalOperator> {

    public abstract void open() throws Throwable;

    public abstract O next() throws Throwable;

    public abstract void close() throws Throwable;

    public abstract IOperatorEnvironment getEnvironment();

    public abstract boolean isOpen();
}
