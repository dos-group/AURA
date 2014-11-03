package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFlatMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.FlatMapFunction;
import de.tuberlin.aura.core.record.OperatorResult;

import java.util.Queue;
import java.util.LinkedList;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public final class FlatMapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Queue<O> elementQueue;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    // TODO : CHANGE FLAT MAP OPERATOR TO ARRAYLIST AND INDEX BASED ACCESS!

    public FlatMapPhysicalOperator(final IExecutionContext context,
                                   final IPhysicalOperator<I> inputOp,
                                   final FlatMapFunction<I,O> function) {

        super(context, inputOp, function);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp.open();

        elementQueue = new LinkedList<>();
    }

    @Override
    public OperatorResult<O> next() throws Throwable {

        while (elementQueue.isEmpty()) {
            OperatorResult<I> input = inputOp.next();

            if (input.marker != StreamMarker.END_OF_STREAM_MARKER) {
                ((IFlatMapFunction<I,O>)function).flatMap(input.element, elementQueue);
            } else {
                return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
            }

        }

        return new OperatorResult<>(elementQueue.poll());

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