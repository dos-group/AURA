package de.tuberlin.aura.core.dataflow.udfs;

import de.tuberlin.aura.core.dataflow.udfs.functions.*;

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

    public static <I,O> FlatMapFunction<I,O> createFlatMapFunction(final Class<FlatMapFunction<I,O>> functionType) {
        // sanity check.
        if (functionType == null)
            throw new IllegalArgumentException("functionType == null");

        try {
            return functionType.getConstructor().newInstance();
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <I,O> GroupMapFunction<I,O> createGroupMapFunction(final Class<GroupMapFunction<I,O>> functionType) {
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

    public static <I,M,O> FoldFunction<I,M,O> createFoldFunction(Class<FoldFunction<I,M,O>> functionType) {
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
