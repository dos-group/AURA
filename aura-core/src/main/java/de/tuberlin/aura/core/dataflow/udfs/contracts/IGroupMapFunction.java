package de.tuberlin.aura.core.dataflow.udfs.contracts;

import java.util.Iterator;
import java.util.Collection;

/**
 *
 */
public interface IGroupMapFunction<I,O> {

    public abstract void map(final Iterator<I> in, Collection<O> c);

}
