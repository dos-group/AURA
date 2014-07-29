package de.tuberlin.aura.core.processing.udfs.contracts;

/**
 *
 */
public interface ISinkFunction<I> {

    public abstract void consume(final I in);
}
