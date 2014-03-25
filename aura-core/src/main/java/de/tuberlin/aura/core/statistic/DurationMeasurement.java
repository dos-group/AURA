package de.tuberlin.aura.core.statistic;

/**
 * Created by teots on 3/12/14.
 */
public class DurationMeasurement extends Measurement {

    private static final boolean MEASURE_TIME_IN_MILLISECONDS = true;

    private long startTime, stopTime, totalTime = 0;

    public DurationMeasurement(MeasurementType type, String description) {
        super(type, description);
    }

    public void startTimer() {
        this.startTime = time();
    }

    public void stopTimer() {
        this.stopTime = time();
        this.totalTime = Math.abs(stopTime - startTime);
    }

    public long elapsedTime() {
        return this.totalTime = Math.abs(time() - this.startTime);
    }

    private long time() {
        return MEASURE_TIME_IN_MILLISECONDS ? System.currentTimeMillis() : System.nanoTime();
    }

    @Override
    public String getHeader() {
        return "TIMESTAMP\tDESCRIPTION\tELAPSED_TIME";
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getTimestamp());
        builder.append("\t");
        builder.append(this.description);
        builder.append("\t");
        builder.append(this.totalTime);

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (!(obj instanceof TimeMeasurement)) return false;

        DurationMeasurement other = (DurationMeasurement) obj;
        if (this.timestamp == other.timestamp && this.type == other.type && this.description.equals(other.description) && this.totalTime == other.totalTime)
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
        result = prime * result + (int) (this.totalTime);

        return result;
    }
}
