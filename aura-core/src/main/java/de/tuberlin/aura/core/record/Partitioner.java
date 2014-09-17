package de.tuberlin.aura.core.record;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 *
 */
public final class Partitioner {

    // disallow instantiation.
    private Partitioner() {}

    /**
     *
     */
    public static enum PartitioningStrategy {

        BROADCAST,              // TODO:

        RANDOM_PARTITIONER,     // TODO:

        HASH_PARTITIONER,

        RANGE_PARTITIONER,

        ROUND_ROBIN_PARTITIONER
    }

    /**
     *
     */
    public static final class PartitionerFactory {

        public static IPartitioner createPartitioner(final PartitioningStrategy strategy, final TypeInformation typeInfo, final int partitioningKeys[][]) {
            // sanity check.
            if (strategy == null)
                throw new IllegalArgumentException("strategy == null");

            switch(strategy) {

                case BROADCAST: {
                    return null;
                }

                case HASH_PARTITIONER: {
                    return new HashPartitioner(typeInfo, partitioningKeys);
                }

                case RANGE_PARTITIONER: {
                    return new RangePartitioner();
                }

                case ROUND_ROBIN_PARTITIONER: {
                    return new RoundRobinPartitioner();
                }

                default: {
                    throw new IllegalStateException("partitioner not supported");
                }
            }
        }
    }

    /**
     *
     */
    public static interface IPartitioner {

        public abstract int partition(final RowRecordModel.Record record, final int receiver);

        public abstract int partition(final Object object, final int receiver);
    }

    /**
     *
     */
    private static abstract class AbstractPartitioner implements IPartitioner {

        public int partition(final RowRecordModel.Record record, final int receiver) {
            throw new NotImplementedException();
        }

        public int partition(final Object object, final int receiver) {
            throw new NotImplementedException();
        }
    }

    /**
     *
     */
    public static class HashPartitioner extends AbstractPartitioner {

        private TypeInformation typeInfo;

        private final int[][] partitioningKeys;

        public HashPartitioner(final TypeInformation typeInfo, final int[][] partitioningKeys) {
            // sanity check.
            if (typeInfo == null)
                throw new IllegalArgumentException("typeInfo == null");

            this.typeInfo = typeInfo;

            this.partitioningKeys = partitioningKeys;
        }

        /*public HashPartitioner(final RowRecordModel.IKeySelector keySelector) {
            // sanity check.
            if (keySelector == null)
                throw new IllegalArgumentException("keySelector == null");

            this.partitioningKeys = keySelector.key();
        }*/

        /*@Override
        public int partition(final RowRecordModel.Record record, final int receiver) {
            int result = 17;
            for(final int fieldIndex : partitionFields)
                result = 31 * result + record.get(fieldIndex).hashCode();
            return (result & Integer.MAX_VALUE) % receiver;
        }*/

        @Override
        public int partition(final Object object, final int receiver) {
            int result = 17;

            if (partitioningKeys != null) {
                for (final int[] selectorChain : partitioningKeys)
                    result = 31 * result + typeInfo.selectField(selectorChain, object).hashCode();
            } else {
                result = 31 * result + object.hashCode();
            }

            return (result & Integer.MAX_VALUE) % receiver;
        }
    }

    /**
     *
     */
    public static class RangePartitioner extends AbstractPartitioner {
    }

    /**
     *
     */
    public static class RoundRobinPartitioner extends AbstractPartitioner {

        private int channelIndex = 0;

        @Override
        public int partition(final RowRecordModel.Record record, final int receiver) {

            channelIndex = ++channelIndex % receiver;

            return channelIndex;
        }
    }
}
