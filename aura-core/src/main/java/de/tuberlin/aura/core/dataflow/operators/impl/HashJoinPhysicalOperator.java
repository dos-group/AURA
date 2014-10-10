package de.tuberlin.aura.core.dataflow.operators.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractBinaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.tuples.Tuple2;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;

/**
 *
 */
public final class HashJoinPhysicalOperator<I1,I2> extends AbstractBinaryPhysicalOperator<I1,I2,Tuple2<I1,I2>> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private TypeInformation input2TypeInfo;

    private int[][] key2Indices;

    private final Map<List<Object>,I1> buildSide;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HashJoinPhysicalOperator(final IExecutionContext context,
                                    final IPhysicalOperator<I1> inputOp1,
                                    final IPhysicalOperator<I2> inputOp2) {

        super(context, inputOp1, inputOp2);

        this.buildSide = new HashMap<>();

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();

        DataflowNodeProperties properties = getContext().getProperties(getOperatorNum());

        int[][] key1Indices = properties.keyIndices1;

        TypeInformation input1TypeInfo = properties.input1Type;

        key2Indices = properties.keyIndices2;

        input2TypeInfo = properties.input2Type;

        // sanity check.
        if (key1Indices.length != key2Indices.length)
            throw new IllegalStateException("joinKeyIndices1.length != joinKeyIndices2.length");
        // TODO: check types!

        OperatorResult<I1> in1 = null;

        // Construct build-side
        inputOp1.open();

        in1 = inputOp1.next();

        while (in1.marker != StreamMarker.END_OF_STREAM_MARKER) {
            final List<Object> key1 = new ArrayList<>(key1Indices.length);

            for (final int[] selectorChain : key1Indices) {
                key1.add(input1TypeInfo.selectField(selectorChain, in1.element));
            }

            buildSide.put(key1, in1.element);

            in1 = inputOp1.next();
        }

        inputOp1.close();

        inputOp2.open();
    }

    @Override
    public OperatorResult<Tuple2<I1,I2>> next() throws Throwable {

        I1 in1 = null;
        OperatorResult<I2> in2 = null;

        while (in1 == null) {

            in2 = inputOp2.next();

            if (in2.marker == StreamMarker.END_OF_STREAM_MARKER) {
                return new OperatorResult<>(null, StreamMarker.END_OF_STREAM_MARKER);
            }

            final List<Object> key2 = new ArrayList<>(key2Indices.length);

            for (final int[] selectorChain : key2Indices) {
                key2.add(input2TypeInfo.selectField(selectorChain, in2.element));
            }

            in1 = buildSide.get(key2);
        }

        return new OperatorResult<>(new Tuple2<>(in1, in2.element));
    }

    @Override
    public void close() throws Throwable {
        super.close();
        inputOp2.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
