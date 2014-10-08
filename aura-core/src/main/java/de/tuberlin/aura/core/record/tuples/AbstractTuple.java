package de.tuberlin.aura.core.record.tuples;

import java.io.Serializable;
import java.util.Iterator;

/**
*
*/
public abstract class AbstractTuple
        implements Serializable, Iterable<Object>, Comparable<AbstractTuple> {

    private static final long serialVersionUID = -1L;

    public abstract <T> T getField(final int pos);

    public abstract <T> void setField(final T value, final int pos);

    public abstract int length();

    public abstract Iterator<Object> iterator();

    public abstract int compareTo(final AbstractTuple o);

    public static AbstractTuple createTuple(int num) {
        switch(num) {
            case 1:
                return new Tuple1<>();
            case 2:
                return new Tuple2<>();
            case 3:
                return new Tuple3<>();
            case 4:
                return new Tuple4<>();
            case 5:
                return new Tuple5<>();
            case 6:
                return new Tuple6<>();
            case 7:
                return new Tuple7<>();
            default:
                throw new IllegalStateException();
        }
    }
}
