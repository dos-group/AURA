package de.tuberlin.aura.core.statistic;

/**
 * Created by teots on 3/12/14.
 */
public class AccumulatedLatencyMeasurement extends Measurement {

    private long minTime = 0l;

    private long maxTime = 0l;

    private double avgTime = 0l;

    private long mdnTime = 0l;

    public AccumulatedLatencyMeasurement(MeasurementType type, String description, long minTime, long maxTime, double avgTime, long mdnTime) {
        super(type, description);
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.avgTime = avgTime;
        this.mdnTime = mdnTime;
    }

    @Override
    public String getHeader() {
        return "TIMESTAMP\tDESCRIPTION\tMIN_TIME\tMAX_TIME\tAVG_TIME\tMEDIAN_TIME";
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getTimestamp());
        builder.append("\t");
        builder.append(this.description);
        builder.append("\t");
        builder.append(this.minTime);
        builder.append("\t");
        builder.append(this.maxTime);
        builder.append("\t");
        builder.append(this.avgTime);
        builder.append("\t");
        builder.append(this.mdnTime);

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (!(obj instanceof TimeMeasurement)) return false;

        AccumulatedLatencyMeasurement other = (AccumulatedLatencyMeasurement) obj;
        if (this.timestamp == other.timestamp && this.type == other.type && this.description.equals(other.description) && this.minTime == other.minTime
                && this.maxTime == other.maxTime && this.avgTime == other.avgTime && this.mdnTime == other.mdnTime)
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
        result = prime * result + (int) (this.minTime);
        result = prime * result + (int) (this.maxTime);
        result = prime * result + (Double.valueOf(this.avgTime).hashCode());
        result = prime * result + (int) (this.mdnTime);

        return result;
    }
}
