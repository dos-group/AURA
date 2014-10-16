package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFilterFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.FilterFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class FilterPhysicalOperator<I> extends AbstractUnaryUDFPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FilterPhysicalOperator(final IExecutionContext context,
                                  final IPhysicalOperator<I> inputOp,
                                  final FilterFunction<I> function) {

        super(context, inputOp, function);
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
    public OperatorResult<I> next() throws Throwable {

        OperatorResult<I> input = inputOp.next();

        while (input.marker != StreamMarker.END_OF_STREAM_MARKER &&
                !((IFilterFunction<I>)function).filter(input.element)) {

            input = inputOp.next();
        }

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
