package de.tuberlin.aura.core.measurement;

public abstract class Measurement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected long timestamp;

    protected MeasurementType type;

    protected String description;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    protected Measurement(MeasurementType type, String description) {
        this.timestamp = System.currentTimeMillis();
        this.type = type;
        this.description = description;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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
