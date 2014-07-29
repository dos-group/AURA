package de.tuberlin.aura.core.processing.udfs.contracts;

/**
 *
 */
public interface IMapFunction<I,O> {

    public abstract O map(final I in);
}
