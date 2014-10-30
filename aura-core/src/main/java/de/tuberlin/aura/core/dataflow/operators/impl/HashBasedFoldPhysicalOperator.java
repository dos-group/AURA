package de.tuberlin.aura.core.dataflow.operators.impl;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.FoldFunction;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.*;

public class HashBasedFoldPhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Map<List<Object>,O> keysToValue;

    private List<O> foldResults;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HashBasedFoldPhysicalOperator(final IExecutionContext context,
                                         final IPhysicalOperator<I> inputOp,
                                         final FoldFunction<I, O> function) {

        super(context, inputOp, function);

        keysToValue = new HashMap<>();
    }


    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {

        super.open();

        FoldFunction<I,O> function = ((FoldFunction<I,O>) this.function);

        TypeInformation inputTypeInfo = getContext().getProperties(this.getOperatorNum()).input1Type;
        int[][] groupKeyIndices = getContext().getProperties(this.getOperatorNum()).groupByKeyIndices;

        inputOp.open();

        OperatorResult<I> input = inputOp.next();

        while (input.marker != StreamMarker.END_OF_STREAM_MARKER) {

            List<Object> keys = new ArrayList<>();

            if (groupKeyIndices != null) {

                for (int i = 0; i < groupKeyIndices.length; i++) {
                    keys.add(i, inputTypeInfo.selectField(groupKeyIndices[i], input.element));
                }

            }

            if (!keysToValue.containsKey(keys)) {
                keysToValue.put(keys, function.empty());
            }

            O value = function.union(keysToValue.get(keys), function.singleton(input.element));
            keysToValue.put(keys, value);

            input = inputOp.next();
        }

        foldResults = new ArrayList<>(keysToValue.values());
    }

    @Override
    public OperatorResult<O> next() throws Throwable {

        if (!foldResults.isEmpty()) {
            return new OperatorResult<>(foldResults.remove(0));
        } else {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }
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
