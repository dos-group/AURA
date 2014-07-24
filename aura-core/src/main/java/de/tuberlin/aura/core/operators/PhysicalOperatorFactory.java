package de.tuberlin.aura.core.operators;

/**
 *
 */
public final class PhysicalOperatorFactory {

    public static AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties,
            final AbstractPhysicalOperator<Object> inputOp1,
            final AbstractPhysicalOperator<Object> inputOp2) {

        // sanity check.
        if (properties == null)
            throw new IllegalArgumentException("properties == null");

        switch(properties.operatorType) {
            case MAP_TUPLE_OPERATOR:
                return new MapPhysicalOperator<>(properties, inputOp1, createUDF(properties.udfFunction));
            case MAP_GROUP_OPERATOR:
                break;
            case FLAT_MAP_TUPLE_OPERATOR:
                break;
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                break;
            case UNION_OPERATOR:
                return new UnionPhysicalOperator<>(properties, inputOp1, inputOp2);
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
                return new UDFSource<>(properties, createUDF(properties.udfFunction));
            case FILE_SOURCE:
                break;
            case STREAM_SOURCE:
                break;
            case UDF_SINK:
                return new UDFSink<>(properties, inputOp1, createUDF(properties.udfFunction));
            case FILE_SINK:
                break;
            case STREAM_SINK:
                break;
        }

        throw new IllegalStateException("'" + properties.operatorType + "' is not defined.");
    }

    public static AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties,
            final AbstractPhysicalOperator<Object> inputOp1) {

        return createPhysicalOperator(properties, inputOp1, null);
    }

    public static AbstractPhysicalOperator<Object> createPhysicalOperator(
            final OperatorProperties properties) {

        return createPhysicalOperator(properties, null, null);
    }

    private static IUnaryUDFFunction<Object,Object> createUDF(final Class<?> udfType) {
        // sanity check.
        if (udfType == null)
            throw new IllegalArgumentException("udfType == null");

        try {
            return (IUnaryUDFFunction<Object,Object>)udfType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
