package de.tuberlin.aura.core.common.utils;

/**
 *
 */
public interface IVisitor<T> {

    public abstract void visit(final T element);
}
