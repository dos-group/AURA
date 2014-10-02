package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;

import java.util.Map;
import java.util.HashMap;


public class DistinctPhysicalOperator<I> extends AbstractUnaryPhysicalOperator<I,I> {

    Map<I,Boolean> hashes;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistinctPhysicalOperator(final IOperatorEnvironment environment,
                                  final IPhysicalOperator<I> inputOp) {

        super(environment, inputOp);

        hashes = new HashMap<>();
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

        while (hashes.containsKey(input) && input != null) {
            input = inputOp.next();
        }

        hashes.put(input, true);

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
