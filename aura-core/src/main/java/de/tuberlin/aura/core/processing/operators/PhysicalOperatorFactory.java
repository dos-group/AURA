package de.tuberlin.aura.core.processing.operators;

import de.tuberlin.aura.core.processing.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.processing.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.processing.operators.impl.*;
import de.tuberlin.aura.core.processing.udfs.FunctionFactory;
import de.tuberlin.aura.core.processing.udfs.functions.MapFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.processing.udfs.functions.SourceFunction;

/**
 *
 */
public final class PhysicalOperatorFactory {

    // Disallow Instantiation.
    private PhysicalOperatorFactory() {}

    // ---------------------------------------------------
    // Static Methods.
    // ---------------------------------------------------

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
                break;
            case FLAT_MAP_TUPLE_OPERATOR:
                break;
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                break;
            case UNION_OPERATOR:
                return new UnionPhysicalOperator<>(environment, inputOp1, inputOp2);
            case DIFFERENCE_OPERATOR:
                break;
            case HASH_JOIN_OPERATOR:
                return new HashJoinPhysicalOperator<>(environment, inputOp1, inputOp2);
            case MERGE_JOIN_OPERATOR:
                break;
            case GROUP_BY_OPERATOR:
                break;
            case SORT_OPERATOR:
                return new SortPhysicalOperator<>(environment, inputOp1);
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
