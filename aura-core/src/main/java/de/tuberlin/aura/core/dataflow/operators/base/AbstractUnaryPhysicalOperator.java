package de.tuberlin.aura.core.dataflow.operators.base;


public abstract class AbstractUnaryPhysicalOperator<I,O> extends AbstractPhysicalOperator<O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IPhysicalOperator<I> inputOp;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractUnaryPhysicalOperator(final IExecutionContext context,
                                         final IPhysicalOperator<I> inputOp) {
        super(context);
        this.inputOp = inputOp;
    }
}
