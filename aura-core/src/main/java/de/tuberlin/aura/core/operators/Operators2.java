package de.tuberlin.aura.core.operators;

import com.google.common.collect.Lists;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.task.spi.IRecordReader;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class Operators2 {

    // Disallow instantiation.
    private Operators2() {}

    /**
     *
     */
    public static interface Visitor<T> {

        public abstract void visit(final T element);
    }

    /**
     *
     */
    public static interface Visitable<T> {

        public abstract void accept(final Visitor<T> visitor);
    }

    /**
     *
     */
    public static final class TopologyBreadthFirstTraverser {

        private static void traverse(final AbstractOperator operator, final Visitor<IOperator> visitor) {
            // sanity check.
            if (operator == null)
                throw new IllegalArgumentException("operator == null");
            if (visitor == null)
                throw new IllegalArgumentException("visitor == null");

            final Set<IOperator> visitedNodes = new HashSet<>();
            final Queue<IOperator> q = new LinkedList<>();

            final Collection<IOperator> startNodes = new ArrayList<>();
            startNodes.add(operator);

            for (final IOperator node : startNodes)
                q.add(node);

            while (!q.isEmpty()) {
                final IOperator node = q.remove();
                node.accept(visitor);

                final Collection<IOperator> nextVisitedNodes;
                    nextVisitedNodes = ((AbstractOperator)node).getChildren();

                for (final IOperator nextNode : nextVisitedNodes) {
                    if (!visitedNodes.contains(nextNode)) {
                        q.add(nextNode);
                        visitedNodes.add(nextNode);
                    }
                }
            }
        }
    }

    /**
     *
     */
    public static enum OperatorType {

        OPERATOR_MAP
    }

    /**
     *
     */
    public static final class OperatorFactory {

        public static <I,O> IOperator createOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor) {
            // sanity check.
            if (operatorDescriptor == null)
                throw new IllegalArgumentException("operatorDescriptor == null");

            switch (operatorDescriptor.operatorType) {

                case OPERATOR_MAP: {

                    final UnaryUDFFunction<I,O> udfFunction;
                    try {
                        udfFunction = (UnaryUDFFunction<I,O>)operatorDescriptor.getUserCodeClasses().get(0).getConstructor().newInstance();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }

                    return new MapOperator<I,O>(operatorDescriptor, new ArrayList<IOperator>(), udfFunction);
                }

                default: {
                    throw new IllegalStateException("operator type not known");
                }
            }
        }
    }

    /**
     *
     */
    public static interface UnaryUDFFunction<I,O> {

        public abstract O apply(final I in);
    }

    /**
     *
     */
    public static interface IOperator extends Serializable, Visitable<IOperator> {

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

        private final Descriptors.OperatorNodeDescriptor operatorDescriptor;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                                final List<IOperator> childOperators) {

            this.childOperators = childOperators;

            this.operatorDescriptor = operatorDescriptor;
        }

        public AbstractOperator(final List<IOperator> childOperators) {
            this(null, childOperators);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
        }

        @Override
        public void close() throws Throwable {
        }

        public void addChild(final IOperator operator) {
            // sanity check.
            if (operator == null)
                throw new IllegalArgumentException("operator == null");

            childOperators.add(operator);
        }

        public List<IOperator> getChildren() {
            return Collections.unmodifiableList(childOperators);
        }

        public Descriptors.OperatorNodeDescriptor getOperatorDescriptor() {
            return operatorDescriptor;
        }

        @Override
        public void accept(final Visitor<IOperator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     */
    public static abstract class AbstractUDFOperator<I,O> extends AbstractOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        protected final UnaryUDFFunction<I,O> udfFunction;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractUDFOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                                   final List<IOperator> childOperators,
                                   final UnaryUDFFunction<I,O> udfFunction) {
            super(operatorDescriptor, childOperators);

            // sanity check.
            if (udfFunction == null)
                throw new IllegalArgumentException("udfFunction == null");

            this.udfFunction = udfFunction;
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Concrete Operator Implementations.
    // ------------------------------------------------------------------------------------------------

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
    public static final class MapOperator<I,O> extends AbstractUDFOperator<I,O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public MapOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                           final List<IOperator> childOperators,
                           final UnaryUDFFunction<I, O> udfFunction) {

            super(operatorDescriptor, childOperators, udfFunction);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable{

            // sanity check.
            if (childOperators.size() != 1)
                throw new IllegalStateException("childOperators.size() != 1");

            childOperators.get(0).open();
        }

        @Override
        public Object next() throws Throwable{
            final I input = (I) childOperators.get(0).next();
            if (input != null)
                return udfFunction.apply(input);
            else
                return null;
        }

        @Override
        public void close() throws Throwable{
            childOperators.get(0).close();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Testing.
    // ------------------------------------------------------------------------------------------------

    public static final class Record1 {

        public Record1() {}

        public int a;
    }

    public static final class Record2 {

        public Record2() {}

        public int a;
    }

    public static final class Record3 {

        public Record3() {}

        public int a;
    }

    public static final class Record4 {

        public Record4() {}

        public int a;
    }

    public static final class MapUDF1 implements UnaryUDFFunction<Record3, Record4> {

        @Override
        public Record4 apply(Record3 in) {
            final Record4 r = new Record4();
            r.a = in.a;
            return r;
        }
    }

    public static final class MapUDF2 implements UnaryUDFFunction<Record2, Record3> {

        @Override
        public Record3 apply(Record2 in) {
            final Record3 r = new Record3();
            r.a = in.a;
            return r;
        }
    }

    public static final class MapUDF3 implements UnaryUDFFunction<Record1, Record2> {

        @Override
        public Record2 apply(Record1 in) {
            final Record2 r = new Record2();
            r.a = in.a;
            return r;
        }
    }

    public static void main(final String[] args) {

        final IOperator map3 = new MapOperator<>(null, null, new MapUDF3());

        final IOperator map2 = new MapOperator<>(null, Lists.newArrayList(map3), new MapUDF2());

        final IOperator map1 = new MapOperator<>(null, Lists.newArrayList(map2), new MapUDF1());


    }
}
