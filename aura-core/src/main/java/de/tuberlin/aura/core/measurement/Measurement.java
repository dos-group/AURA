package de.tuberlin.aura.core.measurement;

import java.util.EnumMap;
import java.util.Map;

/**
 * @author Asterios Katsifodimos
 */
public class Measurement implements Comparable<Measurement> {

    private double measurement = 0;

    private long totalTime = 0;

    private MeasurementType type;

    private long startTime, stopTime = 0;

    private boolean timerStopped = true;

    private static final boolean MEASURE_TIME_IN_MILLISECONDS = true;

    private EnumMap<MeasurementProperty, String> properties = null;

    public Measurement(MeasurementType type) {
        this(type, 0d, true);

        // new org.apache.commons.net.ntp.
    }

    public Measurement(MeasurementType type, double value) {
        this(type, value, true);
    }

    public Measurement(MeasurementType type, boolean registerToGlobalMeasurements) {
        this(type, 0d, registerToGlobalMeasurements);
    }

    public Measurement(MeasurementType type, double value, boolean registerToGlobalMeasurements) {
        this.measurement = value;
        this.type = type;

        if (registerToGlobalMeasurements)
            Measurements.add(this);
    }

    public Measurement addProperty(MeasurementProperty type, String value) {
        if (this.properties == null)
            this.properties = new EnumMap<MeasurementProperty, String>(MeasurementProperty.class);

        this.properties.put(type, value);

        return this;
    }

    public String getProperty(MeasurementProperty type) {
        if (this.properties == null)
            this.properties = new EnumMap<MeasurementProperty, String>(MeasurementProperty.class);

        return this.properties.get(type);
    }

    public Map<MeasurementProperty, String> getProperties() {
        if (this.properties == null)
            this.properties = new EnumMap<MeasurementProperty, String>(MeasurementProperty.class);

        return this.properties;
    }

    public MeasurementType getType() {
        return this.type;
    }

    public double getValue() {
        return this.measurement;
    }

    public void setValue(double value) {
        this.measurement = value;
    }

    public long getTime() {
        return this.totalTime;
    }

    public void startTimer() {
        this.startTime = time();
        this.timerStopped = false;
    }

    public void stopTimer() {
        if (timerStopped)
            return;

        this.stopTime = time();
        this.totalTime += stopTime - startTime;
        this.timerStopped = true;
    }

    public boolean timerStopped() {
        return this.timerStopped;
    }

    public long elapsedTime() {
        if (timerStopped)
            return 0;

        return this.totalTime = time() - this.startTime;
    }

    public void addTime(Measurement time) {
        this.totalTime += time.totalTime;
    }

    public void setTime(long time) {
        this.totalTime = time;
    }

    private static long time() {
        return MEASURE_TIME_IN_MILLISECONDS ? System.currentTimeMillis() : System.nanoTime();
    }

    public String toString() {
        return this.type + ": " + this.getValue();
    }

    @Override
    public int compareTo(Measurement other) {

        if (other.getValue() == this.getValue())
            return 0;
        else if (other.getValue() < this.getValue())
            return 1;
        else
            return -1;
    }
}
