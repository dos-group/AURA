package de.tuberlin.aura.core.operators;

import java.io.Serializable;
import java.util.List;

import de.tuberlin.aura.core.task.spi.IRecordReader;

/**
 *
 */
public final class Operators {

    // Disallow instantiation.
    private Operators() {}

    /**
     *
     */
    public static interface UnaryUDFFunction<I,O> {

        public abstract O apply(final I in);
    }

    /**
     *
     */
    public static interface IOperator extends Serializable {

        public abstract void open() throws Throwable;

        public abstract Object next() throws Throwable;

        public abstract void close() throws Throwable;
    }

    /**
     *
     */
    public static abstract class AbstractOperator implements IOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        protected final List<IOperator> childOperators;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractOperator(final List<IOperator> childOperators) {

            this.childOperators = childOperators;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addChild(final IOperator operator) {
            // sanity check.
            if (operator == null)
                throw new IllegalArgumentException("operator == null");

            childOperators.add(operator);
        }

        public void open() throws Throwable {
        }

        public void close() throws Throwable {
        }
    }

    /**
     *
     */
    public static abstract class AbstractUDFOperator extends AbstractOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        protected final UnaryUDFFunction<Object,Object> udfFunction;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractUDFOperator(final List<IOperator> childOperators, final UnaryUDFFunction<Object,Object> udfFunction) {
            super(childOperators);

            // sanity check.
            if (udfFunction == null)
                throw new IllegalArgumentException("udfFunction == null");

            this.udfFunction = udfFunction;
        }
    }

    /**
     *
     */
    public static final class GateReaderOperator extends AbstractOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IRecordReader recordReader;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public GateReaderOperator(final IRecordReader recordReader) {
            super(null);
            // sanity check.
            if (recordReader == null)
                throw new IllegalArgumentException("recordReader == null");

            this.recordReader = recordReader;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public Object next() {
            return recordReader.readObject();
        }
    }

    /**
     *
     */
    public static final class MapOperator extends AbstractUDFOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IOperator child;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public MapOperator(final List<IOperator> childOperators, final UnaryUDFFunction<Object, Object> udfFunction) {
            super(childOperators, udfFunction);
            // sanity check.
            if (childOperators.size() != 1)
                throw new IllegalStateException("childOperators.size() != 1");

            this.child = childOperators.get(0);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable{
            child.open();
        }

        @Override
        public Object next() throws Throwable{
            return udfFunction.apply(child.next());
        }

        @Override
        public void close() throws Throwable{
            child.close();
        }
    }
}
