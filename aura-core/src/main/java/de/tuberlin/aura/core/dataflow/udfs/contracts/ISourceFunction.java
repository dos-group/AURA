package de.tuberlin.aura.core.dataflow.udfs.contracts;

/**
 *
 */
public interface ISourceFunction<O> {

    public abstract O produce();
}
