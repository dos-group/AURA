package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFilterFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.FilterFunction;


public class FilterPhysicalOperator<I> extends AbstractUnaryUDFPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FilterPhysicalOperator(final IOperatorEnvironment environment,
                                  final IPhysicalOperator<I> inputOp,
                                  final FilterFunction<I> function) {

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
    public I next() throws Throwable {
        I input = inputOp.next();

        while (input != null && !((IFilterFunction<I>)function).filter(input)) {
            input = inputOp.next();
        }

        return input;

        // TODO: should not return each tuple separately, but ALEX: "return an iterator"
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
