package de.tuberlin.aura.core.dataflow.operators.base;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFunction;

/**
 *
 * @param <I>
 * @param <O>
 */
public abstract class AbstractUnaryUDFPhysicalOperator<I,O> extends AbstractUnaryPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final IFunction function;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractUnaryUDFPhysicalOperator(final IOperatorEnvironment environment,
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
        function.setEnvironment(getEnvironment());
        function.create();
    }

    @Override
    public void close() throws Throwable {
        super.close();
        function.release();
    }
}
