package de.tuberlin.aura.core.processing.udfs.contracts;

/**
 *
 */
public interface ISourceFunction<O> {

    public abstract O produce();
}
