package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;


public class HDFSSinkPhysicalOperator <I> extends AbstractUnaryPhysicalOperator<I,Object> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HDFSSinkPhysicalOperator(final IExecutionContext context, final IPhysicalOperator<I> inputOp) {
        super(context, inputOp);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
    }

    @Override
    public Object next() throws Throwable {
        return null;
    }

    @Override
    public void close() throws Throwable {
        super.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
