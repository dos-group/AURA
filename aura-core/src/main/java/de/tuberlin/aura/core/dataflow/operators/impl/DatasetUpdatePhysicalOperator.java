package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.datasets.MutableDataset;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryUDFPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.udfs.functions.UpdateFunction;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.TypeInformation;

import java.util.ArrayList;
import java.util.List;
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

        final UUID datasetID = (UUID)getContext().getProperties().config.get(CO_LOCATION_TASKID);

        // TODO: implement/generify getDataset for mutable datasets
        dataset = (MutableDataset) this.getContext().getRuntime().getTaskManager().getDataset(datasetID);

        this.inputOp.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        final OperatorResult<I> input = inputOp.next();

        if (input.marker == StreamMarker.END_OF_STREAM_MARKER) {
            return new OperatorResult<>(StreamMarker.END_OF_STREAM_MARKER);
        }

        List<Object> keyset = new ArrayList<>(datasetKeyIndices.length);

        for (int i = 0; i < datasetKeyIndices.length; i++) {
            keyset.add(i, inputTypeInfo.selectField(datasetKeyIndices[i], input.element));
        }

        // TODO: change the interface of mutable datasets: contains, get, update.. all operating via keys
        // TODO: finish this!!
//        if (dataset.containsElement(keyset)) {
//
//            O currentState = dataset.get(keyset);
//
//            O newState = ((UpdateFunction<I,O>) function).update(currentState, input.element);
//
//            if (newState != null) {
//                dataset.update(keyset, newState);
//            }
//        }

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
