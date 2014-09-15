package de.tuberlin.aura.core.dataflow.udfs.contracts;

/**
 *
 */
public interface IFilterFunction<I> {

    public abstract boolean filter(final I in);
}
