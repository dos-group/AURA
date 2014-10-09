package de.tuberlin.aura.core.dataflow.operators;

import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.impl.*;
import de.tuberlin.aura.core.dataflow.udfs.FunctionFactory;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;

import java.util.List;


public final class PhysicalOperatorFactory {

    // Disallow Instantiation.
    private PhysicalOperatorFactory() {}

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public static AbstractPhysicalOperator<?> createPhysicalOperator(
            final IExecutionContext context,
            final List<AbstractPhysicalOperator<Object>> inputs) {

        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        final Class<?> udfType = context.getUDFType(context.getProperties().functionTypeName);
        final AbstractPhysicalOperator<Object> inputOp1 = inputs.size() >= 1 ? inputs.get(0) : null;
        final AbstractPhysicalOperator<Object> inputOp2 = inputs.size() >= 2 ? inputs.get(1) : null;

        switch(context.getProperties().type) {
            case MAP_TUPLE_OPERATOR:
                return new MapPhysicalOperator(context, inputOp1, FunctionFactory.createMapFunction((Class<MapFunction<Object,Object>>) udfType));
            case MAP_GROUP_OPERATOR:
                return new GroupMapPhysicalOperator(context, inputOp1, FunctionFactory.createGroupMapFunction((Class<GroupMapFunction<Object,Object>>) udfType));
            case FLAT_MAP_TUPLE_OPERATOR:
                return new FlatMapPhysicalOperator(context, inputOp1, FunctionFactory.createFlatMapFunction((Class<FlatMapFunction<Object,Object>>) udfType));
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                return new FilterPhysicalOperator(context, inputOp1, FunctionFactory.createFilterFunction((Class<FilterFunction<Object>>) udfType));
            case UNION_OPERATOR:
                return new UnionPhysicalOperator<>(context, inputOp1, inputOp2);
            case DIFFERENCE_OPERATOR:
                return new DifferencePhysicalOperator<>(context, inputOp1, inputOp2);
            case DISTINCT_OPERATOR:
                return new DistinctPhysicalOperator<>(context, inputOp1);
            case HASH_JOIN_OPERATOR:
                return new HashJoinPhysicalOperator<>(context, inputOp1, inputOp2);
            case MERGE_JOIN_OPERATOR:
                break;
            case GROUP_BY_OPERATOR:
                return new GroupByPhysicalOperator<>(context, inputOp1);
            case SORT_OPERATOR:
                return new SortPhysicalOperator<>(context, inputOp1);
            case FOLD_OPERATOR:
                return new FoldPhysicalOperator(context, inputOp1, FunctionFactory.createFoldFunction((Class<FoldFunction<Object,Object,Object>>) udfType));
            case REDUCE_OPERATOR:
                break;
            // ---------------------------------------------------
            case UDF_SOURCE:
                return new UDFSourcePhysicalOperator(context, FunctionFactory.createSourceFunction((Class<SourceFunction<Object>>) udfType));
            case UDF_SINK:
                return new UDFSinkPhysicalOperator(context, inputOp1, FunctionFactory.createSinkFunction((Class<SinkFunction<Object>>) udfType));
            case HDFS_SOURCE:
                return new HDFSSourcePhysicalOperator<>(context);
            case HDFS_SINK:
                return new HDFSSinkPhysicalOperator<>(context, inputOp1);
            case STREAM_SOURCE:
                break;
            case STREAM_SINK:
                break;
            // ---------------------------------------------------
            case LOOP_CONTROL_OPERATOR:
                return new LoopControlPhysicalOperator<>(context, inputOp1);
        }

        throw new IllegalStateException("'" + context.getProperties().type + "' is not defined.");
    }

    @SuppressWarnings("unchecked")
    public static AbstractPhysicalOperator<?> createPhysicalOperatorPlan(
            final IExecutionContext context,
            final List<AbstractPhysicalOperator<Object>> inputs) {

        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        final List<DataflowNodeProperties> propertiesList = context.getNodeDescriptor().propertiesList;

        AbstractPhysicalOperator<?> inputOp1 = inputs.size() >= 1 ? inputs.get(0) : null;
        AbstractPhysicalOperator<?> inputOp2 = inputs.size() >= 2 ? inputs.get(1) : null;

        int operatorNum = 0;

        for (final DataflowNodeProperties p : propertiesList) {

            final Class<?> udfType = context.getUDFType(p.functionTypeName);

            switch (context.getProperties().type) {
                case MAP_TUPLE_OPERATOR: {
                    inputOp1 = new MapPhysicalOperator(context, inputOp1, FunctionFactory.createMapFunction((Class<MapFunction<Object, Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case MAP_GROUP_OPERATOR: {
                    inputOp1 = new GroupMapPhysicalOperator(context, inputOp1, FunctionFactory.createGroupMapFunction((Class<GroupMapFunction<Object, Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case FLAT_MAP_TUPLE_OPERATOR: {
                    inputOp1 = new FlatMapPhysicalOperator(context, inputOp1, FunctionFactory.createFlatMapFunction((Class<FlatMapFunction<Object, Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case FLAT_MAP_GROUP_OPERATOR:
                    break;
                case FILTER_OPERATOR: {
                    inputOp1 = new FilterPhysicalOperator(context, inputOp1, FunctionFactory.createFilterFunction((Class<FilterFunction<Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case UNION_OPERATOR: {
                    inputOp1 = new UnionPhysicalOperator(context, inputOp1, inputOp2);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case DIFFERENCE_OPERATOR: {
                    inputOp1 = new DifferencePhysicalOperator(context, inputOp1, inputOp2);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case DISTINCT_OPERATOR: {
                    inputOp1 = new DistinctPhysicalOperator(context, inputOp1);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case HASH_JOIN_OPERATOR: {
                    inputOp1 = new HashJoinPhysicalOperator(context, inputOp1, inputOp2);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case MERGE_JOIN_OPERATOR:
                    break;
                case GROUP_BY_OPERATOR: {
                    inputOp1 = new GroupByPhysicalOperator(context, inputOp1);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case SORT_OPERATOR: {
                    inputOp1 = new SortPhysicalOperator(context, inputOp1);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case FOLD_OPERATOR: {
                    inputOp1 = new FoldPhysicalOperator(context, inputOp1, FunctionFactory.createFoldFunction((Class<FoldFunction<Object, Object, Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case REDUCE_OPERATOR:
                    break;

                // ---------------------------------------------------

                case UDF_SOURCE: {
                    inputOp1 = new UDFSourcePhysicalOperator(context, FunctionFactory.createSourceFunction((Class<SourceFunction<Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case UDF_SINK: {
                    inputOp1 = new UDFSinkPhysicalOperator(context, inputOp1, FunctionFactory.createSinkFunction((Class<SinkFunction<Object>>) udfType));
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case HDFS_SOURCE: {
                    inputOp1 = new HDFSSourcePhysicalOperator(context);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                 }
                case HDFS_SINK: {
                    inputOp1 = new HDFSSinkPhysicalOperator(context, inputOp1);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }
                case STREAM_SOURCE:
                    break;
                case STREAM_SINK:
                    break;

                // ---------------------------------------------------

                case LOOP_CONTROL_OPERATOR: {
                    inputOp1 = new LoopControlPhysicalOperator(context, inputOp1);
                    inputOp1.setOperatorNum(operatorNum);
                    break;
                }

                default:
                    throw new IllegalStateException("'" + p.type + "' is not defined.");
            }

            ++operatorNum;
        }

        return inputOp1;
    }
}
