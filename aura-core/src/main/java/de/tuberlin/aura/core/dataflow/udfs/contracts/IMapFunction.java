package de.tuberlin.aura.core.dataflow.udfs.contracts;

/**
 *
 */
public interface IMapFunction<I,O> {

    public abstract O map(final I in);
}
