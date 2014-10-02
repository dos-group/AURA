package de.tuberlin.aura.core.dataflow.udfs.contracts;

import java.util.Iterator;
import java.util.Collection;


public interface IGroupMapFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void map(final Iterator<I> in, Collection<O> c);
}
