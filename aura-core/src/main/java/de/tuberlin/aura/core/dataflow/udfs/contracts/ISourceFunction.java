package de.tuberlin.aura.core.dataflow.udfs.contracts;


public interface ISourceFunction<O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O produce();
}
