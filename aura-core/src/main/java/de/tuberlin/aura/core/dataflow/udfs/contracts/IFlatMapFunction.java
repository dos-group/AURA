package de.tuberlin.aura.core.dataflow.udfs.contracts;

import java.util.Collection;

public interface IFlatMapFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void flatMap(final I in, Collection<O> c);
}
