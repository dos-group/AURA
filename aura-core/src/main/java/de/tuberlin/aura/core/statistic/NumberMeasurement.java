package de.tuberlin.aura.core.statistic;

/**
 * Created by teots on 3/12/14.
 */
public class NumberMeasurement extends Measurement {

    private static final boolean MEASURE_TIME_IN_MILLISECONDS = true;

    private long number = 0;

    public NumberMeasurement(MeasurementType type, String description, long number) {
        super(type, description);
        this.number = number;
    }

    @Override
    public String getHeader() {
        return "TIMESTAMP\tDESCRIPTION\tNUMBER";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (!(obj instanceof NumberMeasurement)) return false;

        NumberMeasurement other = (NumberMeasurement) obj;
        if (this.timestamp == other.timestamp && this.type == other.type && this.description.equals(other.description) && this.number == other.number)
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
        result = prime * result + (int) (this.number);

        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getTimestamp());
        builder.append("\t");
        builder.append(this.description);
        builder.append("\t");
        builder.append(this.number);

        return builder.toString();
    }
}
