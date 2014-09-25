package de.tuberlin.aura.core.dataflow.operators;

import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.impl.*;
import de.tuberlin.aura.core.dataflow.udfs.FunctionFactory;
import de.tuberlin.aura.core.dataflow.udfs.functions.*;


/**
 *
 */
public final class PhysicalOperatorFactory {

    // Disallow Instantiation.
    private PhysicalOperatorFactory() {}

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public static AbstractPhysicalOperator<?> createPhysicalOperator(
            final IOperatorEnvironment environment,
            final AbstractPhysicalOperator<Object> inputOp1,
            final AbstractPhysicalOperator<Object> inputOp2) {

        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("environment == null");

        switch(environment.getProperties().operatorType) {
            case MAP_TUPLE_OPERATOR:
                return new MapPhysicalOperator(environment, inputOp1, FunctionFactory.createMapFunction((Class<MapFunction<Object,Object>>) environment.getProperties().function));
            case MAP_GROUP_OPERATOR:
                return new GroupMapPhysicalOperator(environment, inputOp1, FunctionFactory.createGroupMapFunction((Class<GroupMapFunction<Object,Object>>) environment.getProperties().function));
            case FLAT_MAP_TUPLE_OPERATOR:
                return new FlatMapPhysicalOperator(environment, inputOp1, FunctionFactory.createFlatMapFunction((Class< FlatMapFunction<Object,Object>>) environment.getProperties().function));
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                return new FilterPhysicalOperator(environment, inputOp1, FunctionFactory.createFilterFunction((Class<FilterFunction<Object>>) environment.getProperties().function));
            case UNION_OPERATOR:
                return new UnionPhysicalOperator<>(environment, inputOp1, inputOp2);
            case DIFFERENCE_OPERATOR:
                return new DifferencePhysicalOperator<>(environment, inputOp1, inputOp2);
            case HASH_JOIN_OPERATOR:
                return new HashJoinPhysicalOperator<>(environment, inputOp1, inputOp2);
            case MERGE_JOIN_OPERATOR:
                break;
            case GROUP_BY_OPERATOR:
                return new GroupByPhysicalOperator<>(environment, inputOp1);
            case SORT_OPERATOR:
                return new SortPhysicalOperator<>(environment, inputOp1);
            case FOLD_OPERATOR:
                return new FoldPhysicalOperator(environment, inputOp1, FunctionFactory.createFoldFunction((Class<FoldFunction<Object,Object,Object>>) environment.getProperties().function));
            case REDUCE_OPERATOR:
                break;
            case UDF_SOURCE:
                return new UDFSourcePhysicalOperator(environment, FunctionFactory.createSourceFunction((Class<SourceFunction<Object>>) environment.getProperties().function));
            case FILE_SOURCE:
                break;
            case STREAM_SOURCE:
                break;
            case UDF_SINK:
                return new UDFSinkPhysicalOperator(environment, inputOp1, FunctionFactory.createSinkFunction((Class<SinkFunction<Object>>) environment.getProperties().function));
            case FILE_SINK:
                break;
            case STREAM_SINK:
                break;
        }

        throw new IllegalStateException("'" + environment.getProperties().operatorType + "' is not defined.");
    }
}
