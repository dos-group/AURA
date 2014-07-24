package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitable;

import java.io.Serializable;

/**
 *
 * @param <O>
 */
public interface IPhysicalOperator<O> extends Serializable, IVisitable<IPhysicalOperator> {

    public abstract void open() throws Throwable;

    public abstract O next() throws Throwable;

    public abstract void close() throws Throwable;

    public abstract OperatorProperties getProperties();
}
