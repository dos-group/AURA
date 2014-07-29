package de.tuberlin.aura.core.processing.udfs.contracts;

/**
 *
 */
public interface IFilterFunction<I> {

    public abstract boolean filter(final I in);
}
