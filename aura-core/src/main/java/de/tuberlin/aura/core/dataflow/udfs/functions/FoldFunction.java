package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFoldFunction;


public abstract class FoldFunction<I,O> extends AbstractFunction implements IFoldFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O empty();

    public abstract O singleton(final I element);

    public abstract O union(O result, final O element);

}
