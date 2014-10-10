package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.ISinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class UDFSinkPhysicalOperator<I> extends AbstractUnaryUDFPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UDFSinkPhysicalOperator(final IExecutionContext context,
                                   final IPhysicalOperator<I> inputOp,
                                   final SinkFunction<I> function) {

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
        final OperatorResult<I> input = inputOp.next();

        if (input.marker != StreamMarker.END_OF_STREAM_MARKER) {
            ((ISinkFunction<I>)function).consume(input.element);
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
