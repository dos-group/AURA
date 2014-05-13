package de.tuberlin.aura.core.measurement;

/**
 *
 */
public class InformationMeasurement extends Measurement {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public String info;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InformationMeasurement(MeasurementType type, String description, String info) {
        super(type, description);
        this.info = info;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public String getHeader() {
        return "TIMESTAMP\tDESCRIPTION\tINFORMATION";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        if (!(obj instanceof InformationMeasurement))
            return false;

        InformationMeasurement other = (InformationMeasurement) obj;
        if (this.timestamp == other.timestamp && this.type == other.type && this.description.equals(other.description)
                && this.info.equals(other.info))
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
        result = prime * result + this.info.hashCode();

        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getTimestamp());
        builder.append("\t");
        builder.append(this.description);
        builder.append("\t");
        builder.append(this.info);

        return builder.toString();
    }
}
