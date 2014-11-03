package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.datasets.MutableDataset;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.UpdateFunction;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.UUID;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public class DatasetUpdatePhysicalOperator<I,O> extends AbstractUnaryUDFPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private TypeInformation inputTypeInfo;

    private int[][] datasetKeyIndices;

    private MutableDataset<O> dataset;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DatasetUpdatePhysicalOperator(final IExecutionContext context,
                                         final IPhysicalOperator<I> inputOp,
                                         final UpdateFunction<I, O> function) {

        super(context, inputOp, function);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {

        super.open();

        datasetKeyIndices = getContext().getProperties().datasetKeyIndices;
        inputTypeInfo = getContext().getProperties(this.getOperatorNum()).input1Type;

        final UUID datasetID = (UUID)getContext().getProperties().config.get(CO_LOCATION_TASK_NAME);

        dataset = this.getContext().getRuntime().getTaskManager().getMutableDataset(datasetID);

        this.inputOp.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        final OperatorResult<I> input = inputOp.next();

        if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }

        Object[] keys = new Object[datasetKeyIndices.length];

        for (int i = 0; i < datasetKeyIndices.length; i++) {
            keys[i] = inputTypeInfo.selectField(datasetKeyIndices[i], input.element);
        }

        if (dataset.containsElement(keys)) {

            O currentState = dataset.get(keys);

            O newState = ((UpdateFunction<I,O>) function).update(currentState, input.element);

            if (newState != null) {
                dataset.update(keys, newState);
            }
        }

        return input;
    }

    @Override
    public void close() throws Throwable {
        super.close();

        this.inputOp.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
