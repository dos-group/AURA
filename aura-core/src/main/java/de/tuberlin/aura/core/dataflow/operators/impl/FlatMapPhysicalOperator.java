package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IFlatMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.FlatMapFunction;

import java.util.Queue;
import java.util.LinkedList;


public final class FlatMapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Queue<O> elementQueue;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FlatMapPhysicalOperator(final IExecutionContext environment,
                                   final IPhysicalOperator<I> inputOp,
                                   final FlatMapFunction<I,O> function) {

        super(environment, inputOp, function);
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
    public O next() throws Throwable {

        I input;

        // FIXME: flatmap should be able to return/write all returned tuples at once, maybe also through a Collector

        while (elementQueue.isEmpty()) {
            input = inputOp.next();

            if (input != null) {
                ((IFlatMapFunction<I,O>)function).flatMap(input, elementQueue);
            } else {
                return null;
            }
        }

        return elementQueue.poll();
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
