package de.tuberlin.aura.core.operators;

/**
 *
 */
public interface IUnaryUDFFunction<I,O> {

    public abstract O apply(final I in);
}
