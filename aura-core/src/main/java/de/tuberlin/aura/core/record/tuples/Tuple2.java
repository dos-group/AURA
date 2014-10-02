package de.tuberlin.aura.core.record.tuples;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

/**
 *
 */
public final class Tuple2<T1, T2> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T1 _1;

    public T2 _2;

    public Tuple2() {
        this(null, null);
    }

    public Tuple2(final T1 _1, final T2 _2) {

        this._1 = _1;

        this._2 = _2;
    }

    public Tuple2(final Tuple2<T1, T2> t) {
        this(t._1, t._2);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._1;
            case 1: return (T) this._2;
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
            case 1:
                this._2 = (T2) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 2;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_1, _2}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple2<T1, T2> o = (Tuple2<T1, T2>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_1 instanceof Comparable)
            cc.compare((Comparable<?>) _1, (Comparable<?>) o._1);
        if (_2 instanceof Comparable)
            cc.compare((Comparable<?>) _2, (Comparable<?>) o._2);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
        result = prime * result + ((_2 == null) ? 0 : _2.hashCode());
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
        final Tuple2<T1, T2> other = (Tuple2<T1, T2>) obj;
        if (_1 == null) {
            if (other._1 != null)
                return false;
        } else if (!_1.equals(other._1))
            return false;
        if (_2 == null) {
            if (other._2 != null)
                return false;
        } else if (!_2.equals(other._2))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _1 + "," + _2 + ")";
    }
}
