package de.tuberlin.aura.core.statistic;

/**
 * Created by teots on 3/12/14.
 */
public class ThroughputMeasurement extends Measurement {

    private static final boolean MEASURE_TIME_IN_MILLISECONDS = true;

    private long throughput = 0;

    /**
     * TODO: Make the size of the time slot variable.
     * 
     * @param type
     * @param description
     * @param throughput In bytes for the given time slot. The size of the slot must be 1 s.
     * @param timeslot
     */
    public ThroughputMeasurement(MeasurementType type, String description, long throughput, long timeslot) {
        super(type, description);
        this.throughput = throughput;
        this.timestamp = timeslot;
    }

    @Override
    public String getHeader() {
        return "TIMESTAMP\tDESCRIPTION\tTHROUGHPUT";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        if (!(obj instanceof ThroughputMeasurement))
            return false;

        ThroughputMeasurement other = (ThroughputMeasurement) obj;
        if (this.timestamp == other.timestamp && this.type == other.type && this.description.equals(other.description)
                && this.throughput == other.throughput)
            return true;

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.timestamp);
        result = prime * result + (this.type.hashCode());
        result = prime * result + (this.description.hashCode());
        result = prime * result + (int) (this.throughput);

        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getTimestamp());
        builder.append("\t");
        builder.append(this.description);
        builder.append("\t");
        builder.append((double) this.throughput * 8.0d / 1024.0d / 1024.0d);
        builder.append(" Mbit/s");

        return builder.toString();
    }
}
