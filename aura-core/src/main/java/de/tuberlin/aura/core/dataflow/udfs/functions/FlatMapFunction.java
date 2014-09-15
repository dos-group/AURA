package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFlatMapFunction;

import java.util.Collection;

public abstract class FlatMapFunction<I,O> extends AbstractFunction implements IFlatMapFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void flatMap(final I in, Collection<O> c);

}