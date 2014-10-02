package de.tuberlin.aura.core.dataflow.udfs.contracts;

public interface IFoldFunction<I,M,O> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract O initialValue();

    public abstract M map(final I in1);

    public abstract O add(O currentValue, final M mRes);
}
