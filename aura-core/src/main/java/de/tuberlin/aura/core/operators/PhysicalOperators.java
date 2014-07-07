package de.tuberlin.aura.core.operators;

import java.io.Serializable;

import de.tuberlin.aura.core.common.utils.Visitable;
import de.tuberlin.aura.core.common.utils.Visitor;

/**
 *
 */
public class PhysicalOperators {

    /**
     *
     * @param <O>
     */
    public static interface IPhysicalOperator<O> extends Serializable, Visitable<IPhysicalOperator> {

        public abstract void open() throws Throwable;

        public abstract O next() throws Throwable;

        public abstract void close() throws Throwable;

        public abstract OperatorProperties getProperties();
    }

    /**
     *
     * @param <O>
     */
    public static abstract class AbstractPhysicalOperator<O> implements IPhysicalOperator<O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final OperatorProperties properties;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public AbstractPhysicalOperator(final OperatorProperties properties) {
            this.properties = properties;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
        }

        @Override
        public O next() throws Throwable {
            return null;
        }

        @Override
        public void close() throws Throwable {
        }

        @Override
        public OperatorProperties getProperties() {
            return properties;
        }
    }


    /**
     *
     * @param <I>
     * @param <O>
     */
    public static abstract class AbstractUnaryPhysicalOperator<I,O> extends AbstractPhysicalOperator<O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        protected final IPhysicalOperator<I> inputOp;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        protected AbstractUnaryPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I> inputOp) {
            super(properties);
            this.inputOp = inputOp;
        }
    }

    /**
     *
     * @param <I>
     * @param <O>
     */
    public static abstract class AbstractUnaryUDFPhysicalOperator<I,O> extends AbstractUnaryPhysicalOperator<I,O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        protected final UnaryUDFFunction<I,O> udfFunction;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        protected AbstractUnaryUDFPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I> inputOp, final UnaryUDFFunction<I, O> udfFunction) {
            super(properties, inputOp);

            this.udfFunction = udfFunction;
        }
    }

    /**
     *
     * @param <I1>
     * @param <I2>
     * @param <O>
     */
    public static abstract class AbstractBinaryPhysicalOperator<I1,I2,O> extends AbstractPhysicalOperator<O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        protected final IPhysicalOperator<I1> inputOp1;

        protected final IPhysicalOperator<I2> inputOp2;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        protected AbstractBinaryPhysicalOperator(final OperatorProperties properties, final IPhysicalOperator<I1> inputOp1, final IPhysicalOperator<I2> inputOp2) {
            super(properties);

            // sanity check.
            if (inputOp1 == null)
                throw new IllegalArgumentException("inputOp1 == null");
            if (inputOp2 == null)
                throw new IllegalArgumentException("inputOp2 == null");

            this.inputOp1 = inputOp1;

            this.inputOp2 = inputOp2;
        }
    }

    //----------------------------------------------------------------------------

    /**
     *
     * @param <O>
     */
    /*public static abstract class BaseSource<O> extends AbstractPhysicalOperator<O> {

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public BaseSource(final OperatorProperties properties) {
            super(properties);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
        }

        @Override
        public O next() throws Throwable {
            return null;
        }

        @Override
        public void close() throws Throwable {
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }*/

    /**
     *
     * @param <O>
     */
    public static class UDFSource<O> extends AbstractUnaryUDFPhysicalOperator<Object,O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public UDFSource(final OperatorProperties properties, final UnaryUDFFunction<Object, O> udfFunction) {
            super(properties, null, udfFunction);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
        }

        @Override
        public O next() throws Throwable {
            return udfFunction.apply(null);
        }

        @Override
        public void close() throws Throwable {
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     * @param <I>
     */
    /*public static class BaseSink<I> extends AbstractUnaryPhysicalOperator<I,Void> {

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public BaseSink(final OperatorProperties properties, final IPhysicalOperator<I> inputOp) {
            super(properties, inputOp);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            inputOp.open();
        }

        @Override
        public Void next() throws Throwable {
            inputOp.next();
            return null;
        }

        @Override
        public void close() throws Throwable {
            inputOp.close();
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }*/

    /**
     *
     * @param <I>
     */
    public static class UDFSink<I> extends AbstractUnaryUDFPhysicalOperator<I,Object> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public UDFSink(final OperatorProperties properties, final IPhysicalOperator<I> inputOp, final UnaryUDFFunction<I,Object> udfFunction) {
            super(properties, inputOp, udfFunction);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            inputOp.open();
        }

        @Override
        public Void next() throws Throwable {
            final I input = inputOp.next();
            if (input != null)
                udfFunction.apply(input);
            return null;
        }

        @Override
        public void close() throws Throwable {
            inputOp.close();
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     * @param <I>
     * @param <O>
     */
    public static final class MapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public MapPhysicalOperator(final OperatorProperties properties,
                                   final IPhysicalOperator<I> inputOp,
                                   final UnaryUDFFunction<I, O> udfFunction) {

            super(properties, inputOp, udfFunction);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            inputOp.open();
        }

        @Override
        public O next() throws Throwable {
            final I input = inputOp.next();
            if (input != null)
                return udfFunction.apply(input);
            else
                return null;
        }

        @Override
        public void close() throws Throwable {
            inputOp.close();
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     * @param <I>
     */
    public static final class UnionPhysicalOperator<I> extends AbstractBinaryPhysicalOperator<I,I,I> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private boolean input1Closed = false;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public UnionPhysicalOperator(final OperatorProperties properties,
                                     final IPhysicalOperator<I> inputOp1,
                                     final IPhysicalOperator<I> inputOp2) {

            super(properties, inputOp1, inputOp2);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            inputOp1.open();
        }

        @Override
        public I next() throws Throwable {

            if (!input1Closed) {
                final I input1 = inputOp1.next();

                if (input1 != null) {
                    return input1;
                } else {
                    inputOp1.close();
                    input1Closed = true;
                    inputOp2.open();
                }
            }

            return inputOp2.next();

            /*final I output;

            if (switchState)
              output = inputOp1.next();
            else
              output = inputOp2.next();

            return output;*/
        }

        @Override
        public void close() throws Throwable {
            inputOp2.close();
        }

        @Override
        public void accept(final Visitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }
}
