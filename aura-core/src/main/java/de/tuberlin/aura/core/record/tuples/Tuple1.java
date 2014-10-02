package de.tuberlin.aura.core.record.tuples;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

/**
 *
 */
public final class Tuple1<T1> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T1 _1;

    public Tuple1() {
        this((T1)null);
    }

    public Tuple1(final T1 _1) {
        this._1 = _1;
    }

    public Tuple1(final Tuple1<T1> t) {
        this(t._1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._1;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(final T value, final int pos) {
        switch(pos) {
            case 0:
                this._1 = (T1) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_1}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple1<T1> o = (Tuple1<T1>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_1 instanceof Comparable)
            cc.compare((Comparable<?>) _1, (Comparable<?>) o._1);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        final Tuple1<T1> other = (Tuple1<T1>) obj;
        if (_1 == null) {
            if (other._1 != null)
                return false;
        } else if (!_1.equals(other._1))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _1 + ")";
    }
}
