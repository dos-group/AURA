package de.tuberlin.aura.core.dataflow.types;

import java.util.Iterator;

/**
 *
 */
public interface IDataset<E> {

    public Iterator<E> getIterator();
}
