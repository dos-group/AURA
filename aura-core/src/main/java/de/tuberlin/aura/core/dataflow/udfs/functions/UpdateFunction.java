package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IUpdateFunction;

public abstract class UpdateFunction<I,O> extends AbstractFunction implements IUpdateFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O update(final O state, final I update);

}
