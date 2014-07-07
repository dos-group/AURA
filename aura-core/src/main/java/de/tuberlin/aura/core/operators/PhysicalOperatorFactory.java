package de.tuberlin.aura.core.operators;

/**
 *
 */
public final class PhysicalOperatorFactory {

    public static PhysicalOperators.AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties,
            final PhysicalOperators.AbstractPhysicalOperator<Object> inputOp1,
            final PhysicalOperators.AbstractPhysicalOperator<Object> inputOp2) {

        // sanity check.
        if (properties == null)
            throw new IllegalArgumentException("properties == null");

        switch(properties.operatorType) {
            case MAP_TUPLE_OPERATOR:
                return new PhysicalOperators.MapPhysicalOperator<>(properties, inputOp1, createUDF(properties.udfFunction));
            case MAP_GROUP_OPERATOR:
                break;
            case FLAT_MAP_TUPLE_OPERATOR:
                break;
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                break;
            case UNION_OPERATOR:
                return new PhysicalOperators.UnionPhysicalOperator<>(properties, inputOp1, inputOp2);
            case DIFFERENCE_OPERATOR:
                break;
            case HASH_JOIN_OPERATOR:
                break;
            case MERGE_JOIN_OPERATOR:
                break;
            case GROUP_BY_OPERATOR:
                break;
            case SORT_OPERATOR:
                break;
            case REDUCE_OPERATOR:
                break;
            case UDF_SOURCE:
                return new PhysicalOperators.UDFSource<>(properties, createUDF(properties.udfFunction));
            case FILE_SOURCE:
                break;
            case STREAM_SOURCE:
                break;
            case UDF_SINK:
                return new PhysicalOperators.UDFSink<>(properties, inputOp1, createUDF(properties.udfFunction));
            case FILE_SINK:
                break;
            case STREAM_SINK:
                break;
        }

        throw new IllegalStateException("'" + properties.operatorType + "' is not defined.");
    }

    public static PhysicalOperators.AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties,
            final PhysicalOperators.AbstractPhysicalOperator<Object> inputOp1) {

        return createPhysicalOperator(properties, inputOp1, null);
    }

    public static PhysicalOperators.AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties) {

        return createPhysicalOperator(properties, null, null);
    }

    private static UnaryUDFFunction<Object,Object> createUDF(final Class<?> udfType) {
        // sanity check.
        if (udfType == null)
            throw new IllegalArgumentException("udfType == null");

        try {
            return (UnaryUDFFunction<Object,Object>)udfType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
