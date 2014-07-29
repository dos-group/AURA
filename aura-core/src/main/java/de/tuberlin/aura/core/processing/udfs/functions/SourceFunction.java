package de.tuberlin.aura.core.processing.udfs.functions;

import de.tuberlin.aura.core.processing.udfs.contracts.ISourceFunction;

/**
 *
 */
public abstract class SourceFunction<O> extends AbstractFunction implements ISourceFunction<O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O produce();
}