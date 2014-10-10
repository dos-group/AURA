package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IGroupMapFunction;

import java.util.Collection;
import java.util.Iterator;

public abstract class GroupMapFunction<I,O> extends AbstractFunction implements IGroupMapFunction<I,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void map(final Iterator<I> in, Collection<O> output);
}