package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFoldFunction;


public abstract class FoldFunction<I,O> extends AbstractFunction implements IFoldFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O empty();

    public abstract O singleton(final I in);

    public abstract O union(O currentValue, final O mRes);

}
