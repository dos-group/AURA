package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;

/**
 *
 */
public class FoldPhysicalOperator<I,M,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {
    public FoldPhysicalOperator(final IOperatorEnvironment environment,
                                final IPhysicalOperator<I> inputOp,
                                final FoldFunction<I, M, O> function) {

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
    public O next() throws Throwable {

        if (!this.isOpen()) {
            return null;
        }

        FoldFunction<I,M,O> function = ((FoldFunction<I,M,O>) this.function);

        O value = function.initialValue();

        I input = inputOp.next();

        while (input != null) {

            if (input == GroupEndMarker.class) {
                return value;
            }

            value = function.add(value, function.map(input));
            input = inputOp.next();
        }

        this.close();

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
