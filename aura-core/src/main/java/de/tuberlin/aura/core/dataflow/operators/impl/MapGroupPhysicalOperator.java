package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.*;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.contracts.IGroupMapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.GroupMapFunction;
import de.tuberlin.aura.core.record.OperatorInputIterator;
import de.tuberlin.aura.core.record.OperatorResult;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;

public class MapGroupPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private List<O> results;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public MapGroupPhysicalOperator(final IExecutionContext context,
                                    final IPhysicalOperator<I> inputOp,
                                    final GroupMapFunction<I, O> function) {

        super(context, inputOp, function);

        results = new ArrayList<>();
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
    public OperatorResult<O> next() throws Throwable {

        while (results.isEmpty()) {

            OperatorInputIterator<I> it = new OperatorInputIterator<>(inputOp);

            if (it.endOfStream()) {
                return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
            }

            ((IGroupMapFunction<I, O>) function).map(it, results);

            if (it.endOfGroup()) {
                results.add(null);
            }
        }

        OperatorResult<O> result = new OperatorResult<>(results.remove(0));

        if (result.element == null) {
            return new OperatorResult<>(StreamMarker.END_OF_GROUP_MARKER);
        }

        return result;
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
