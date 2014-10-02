package de.tuberlin.aura.core.record.tuples;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

/**
 *
 */
public final class Tuple5<T1, T2, T3, T4, T5> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T1 _1;

    public T2 _2;

    public T3 _3;

    public T4 _4;

    public T5 _5;

    public Tuple5() {
        this(null, null, null, null, null);
    }

    public Tuple5(final T1 _1, final T2 _2, final T3 _3, final T4 _4, final T5 _5) {

        this._1 = _1;

        this._2 = _2;

        this._3 = _3;

        this._4 = _4;

        this._5 = _5;
    }

    public Tuple5(final Tuple5<T1, T2, T3, T4, T5> t) {
        this(t._1, t._2, t._3, t._4, t._5);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._1;
            case 1: return (T) this._2;
            case 2: return (T) this._3;
            case 3: return (T) this._4;
            case 4: return (T) this._5;
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
            case 2:
                this._3 = (T3) value;
                break;
            case 3:
                this._4 = (T4) value;
                break;
            case 4:
                this._5 = (T5) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 5;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_1, _2, _3, _4, _5}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple5<T1, T2, T3, T4, T5> o = (Tuple5<T1, T2, T3, T4, T5>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_1 instanceof Comparable)
            cc.compare((Comparable<?>) _1, (Comparable<?>) o._1);
        if (_2 instanceof Comparable)
            cc.compare((Comparable<?>) _2, (Comparable<?>) o._2);
        if (_3 instanceof Comparable)
            cc.compare((Comparable<?>) _3, (Comparable<?>) o._3);
        if (_4 instanceof Comparable)
            cc.compare((Comparable<?>) _4, (Comparable<?>) o._4);
        if (_5 instanceof Comparable)
            cc.compare((Comparable<?>) _5, (Comparable<?>) o._5);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
        result = prime * result + ((_2 == null) ? 0 : _2.hashCode());
        result = prime * result + ((_3 == null) ? 0 : _3.hashCode());
        result = prime * result + ((_4 == null) ? 0 : _4.hashCode());
        result = prime * result + ((_5 == null) ? 0 : _5.hashCode());
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
        final Tuple5<T1, T2, T3, T4, T5> other = (Tuple5<T1, T2, T3, T4, T5>) obj;
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
        if (_3 == null) {
            if (other._3 != null)
                return false;
        } else if (!_3.equals(other._3))
            return false;
        if (_4 == null) {
            if (other._4 != null)
                return false;
        } else if (!_4.equals(other._4))
            return false;
        if (_5 == null) {
            if (other._5 != null)
                return false;
        } else if (!_5.equals(other._5))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _1 + "," + _2 + "," + _3 + "," + _4 +  "," + _5 + ")";
    }
}
