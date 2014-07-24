package de.tuberlin.aura.core.common.utils;

/**
 *
 */
public interface IVisitable<T> {

    public abstract void accept(final IVisitor<T> visitor);
}
