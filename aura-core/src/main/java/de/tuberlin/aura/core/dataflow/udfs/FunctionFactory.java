package de.tuberlin.aura.core.dataflow.udfs;

import de.tuberlin.aura.core.dataflow.udfs.functions.FilterFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.MapFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SinkFunction;
import de.tuberlin.aura.core.dataflow.udfs.functions.SourceFunction;

/**
 *
 */
public final class FunctionFactory {

    public static <I,O> MapFunction<I,O> createMapFunction(final Class<MapFunction<I,O>> functionType) {
        // sanity check.
        if (functionType == null)
            throw new IllegalArgumentException("functionType == null");

        try {
            return functionType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <I> FilterFunction<I> createFilterFunction(final Class<FilterFunction<I>> functionType) {
        // sanity check.
        if (functionType == null)
            throw new IllegalArgumentException("functionType == null");

        try {
            return functionType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <O> SourceFunction<O> createSourceFunction(final Class<SourceFunction<O>> functionType) {
        // sanity check.
        if (functionType == null)
            throw new IllegalArgumentException("functionType == null");

        try {
            return functionType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <I> SinkFunction<I> createSinkFunction(final Class<SinkFunction<I>> functionType) {
        // sanity check.
        if (functionType == null)
            throw new IllegalArgumentException("functionType == null");

        try {
            return functionType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
