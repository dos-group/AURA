package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IGroupMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.GroupMapFunction;
import de.tuberlin.aura.core.record.GroupedOperatorInputIterator;

import java.util.*;

/**
 *
 */
public class GroupMapPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Queue<O> elementQueue;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public GroupMapPhysicalOperator(final IOperatorEnvironment environment,
                                    final IPhysicalOperator<I> inputOp,
                                    final GroupMapFunction<I, O> function) {

        super(environment, inputOp, function);

        elementQueue = new LinkedList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();
        inputOp.open();
    }

    @Override
    public O next() throws Throwable {

        while (elementQueue.isEmpty()) {

            GroupedOperatorInputIterator<I> it = new GroupedOperatorInputIterator<>(inputOp);

            if (it.isDrained()) {
                this.close();
                return null;
            }

            ((IGroupMapFunction<I, O>) function).map(it, elementQueue);

            if (!it.isDrained()) {
                // add null as the group is finished
                elementQueue.add(null);
            }
        }

        // FIXME: groupmap should be able to return/write all returned tuples at once (through a Collector)
        return elementQueue.poll();
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }

}
