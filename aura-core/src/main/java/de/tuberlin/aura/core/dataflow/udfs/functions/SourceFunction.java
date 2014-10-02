package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.ISourceFunction;


public abstract class SourceFunction<O> extends AbstractFunction implements ISourceFunction<O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O produce();
}
