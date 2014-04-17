package de.tuberlin.aura.core.statistic;

/**
 * @author Asterios Katsifodimos
 */
public abstract class Measurement {

    protected long timestamp;

    protected MeasurementType type;

    protected String description;

    protected Measurement(MeasurementType type, String description) {
        this.timestamp = System.currentTimeMillis();
        this.type = type;
        this.description = description;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public MeasurementType getType() {
        return this.type;
    }

    public abstract String getHeader();

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();
}
