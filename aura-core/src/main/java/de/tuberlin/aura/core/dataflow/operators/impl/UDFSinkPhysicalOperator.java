package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.ISinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;


public class UDFSinkPhysicalOperator<I> extends AbstractUnaryUDFPhysicalOperator<I,Object> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSinkPhysicalOperator(final IOperatorEnvironment environment,
                                   final IPhysicalOperator<I> inputOp,
                                   final SinkFunction<I> function) {

        super(environment, inputOp, function);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp.open();
    }

    @Override
    public Object next() throws Throwable {
        final I input = inputOp.next();
        if (input != null)
            ((ISinkFunction<I>)function).consume(input);
        return input;
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
