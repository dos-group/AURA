package de.tuberlin.aura.core.dataflow.operators.base;

import java.io.Serializable;
import java.util.List;

import de.tuberlin.aura.core.common.utils.IVisitable;
import de.tuberlin.aura.core.record.OperatorResult;


public interface IPhysicalOperator<O> extends Serializable, IVisitable<IPhysicalOperator> {

    public abstract void open() throws Throwable;

    public abstract OperatorResult<O> next() throws Throwable;

    public abstract void close() throws Throwable;

    public abstract IExecutionContext getContext();

    public abstract boolean isOpen();

    public abstract void setOutputGates(final List<Integer> gateIndices);

    public abstract List<Integer> getOutputGates();

    public abstract void setOperatorNum(final int operatorNum);

    public abstract int getOperatorNum();
}
