package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 */
public class PhysicalOperators {


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
        public void accept(final IVisitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }*/

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
        public void accept(final IVisitor<IPhysicalOperator> visitor) {
            visitor.visit(this);
        }
    }*/

}
