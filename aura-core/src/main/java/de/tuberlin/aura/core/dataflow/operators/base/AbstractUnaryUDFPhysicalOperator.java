package de.tuberlin.aura.core.dataflow.operators.base;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFunction;


public abstract class AbstractUnaryUDFPhysicalOperator<I,O> extends AbstractUnaryPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IFunction function;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractUnaryUDFPhysicalOperator(final IExecutionContext environment,
                                            final IPhysicalOperator<I> inputOp,
                                            final IFunction function) {

        super(environment, inputOp);

        this.function = function;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        function.setEnvironment(getContext());
        function.create();
    }

    @Override
    public void close() throws Throwable {
        super.close();
        function.release();
    }
}
