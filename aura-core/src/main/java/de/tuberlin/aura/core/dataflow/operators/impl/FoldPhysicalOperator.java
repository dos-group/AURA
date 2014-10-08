package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.record.GroupedInputIterator;


public class FoldPhysicalOperator<I,M,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {
    public FoldPhysicalOperator(final IExecutionContext context,
                                final IPhysicalOperator<I> inputOp,
                                final FoldFunction<I, O> function) {

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
    public O next() throws Throwable {

        if (!this.isOpen()) {
            return null;
        }

        GroupedInputIterator<I> inputIterator = new GroupedInputIterator<>(inputOp);

        FoldFunction<I,O> function = ((FoldFunction<I,O>) this.function);

        O value = function.empty();

        while (inputIterator.hasNext()) {

            I input = inputIterator.next();

            value = function.union(value, function.singleton(input));
        }

        if (inputIterator.isDrained()) {
            this.close();
        }

        return value;
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
