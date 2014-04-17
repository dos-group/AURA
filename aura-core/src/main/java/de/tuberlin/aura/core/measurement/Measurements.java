package de.tuberlin.aura.core.measurement;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author Asterios Katsifodimos
 */
public class Measurements {

    private static final EnumMap<MeasurementType, List<Measurement>> measurements =
            new EnumMap<MeasurementType, List<Measurement>>(MeasurementType.class);

    private static final Object modificationLock = new Object();

    private static int exportCount = 0;

    protected synchronized static void add(Measurement measurement) {
        List<Measurement> stats;

        synchronized (modificationLock) {
            if ((stats = measurements.get(measurement.getType())) == null) {
                stats = Collections.synchronizedList(new LinkedList<Measurement>());
                measurements.put(measurement.getType(), stats);
            }
            stats.add(measurement);
        }
    }

    public synchronized static long getTotalTime(MeasurementType type) {
        long sum = 0;

        List<Measurement> ms = measurements.get(type);

        if (ms == null || ms.size() == 0)
            return 0l;

        for (Measurement m : ms) {
            sum += m.getTime();
        }

        return sum;
    }

    public synchronized static double getTotal(MeasurementType type) {
        double sum = 0;

        List<Measurement> ms = measurements.get(type);

        if (ms == null || ms.isEmpty())
            return 0d;

        for (Measurement m : ms) {
            sum += m.getValue();
        }

        return sum;
    }


    public synchronized static double getAverage(MeasurementType type) {
        long sum = 0;

        List<Measurement> ms = measurements.get(type);

        if (ms == null || ms.isEmpty())
            return 0d;

        for (Measurement m : ms) {
            sum += m.getValue();
        }

        return sum / ((double) ms.size());
    }


    public synchronized static double getMax(MeasurementType type) {
        List<Measurement> ms = measurements.get(type);

        if (ms == null || ms.isEmpty())
            return Double.MIN_VALUE;

        return Collections.max(ms, new MeasurementComparator()).getValue();
    }


    public synchronized static double getMin(MeasurementType type) {
        List<Measurement> ms = measurements.get(type);

        if (ms == null || ms.isEmpty())
            return Double.MAX_VALUE;

        return Collections.min(ms, new MeasurementComparator()).getValue();
    }


    public synchronized static void clear() {
        synchronized (modificationLock) {
            measurements.clear();
        }
    }

    public synchronized static void clear(MeasurementType type) {
        synchronized (modificationLock) {
            measurements.remove(type);
        }
    }


    protected static class MeasurementComparator implements Comparator<Measurement> {

        @Override
        public int compare(Measurement o1, Measurement o2) {
            if (o1.getValue() == o2.getValue())
                return 0;
            else if (o1.getValue() < o2.getValue())
                return 1;
            else
                return -1;
        }
    }

    public synchronized static void export(MeasurementType type, MeasurementProperty propertiesToPrint[], OutputStream output) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(new BufferedOutputStream(output));

        if (measurements.get(type) == null || measurements.get(type).isEmpty())
            return;

        Arrays.sort(propertiesToPrint);

        // Create the header of the file:
        out.write("MEASUREMENT_TYPE\tMEASUREMENT_VALUE\tMEASUREMENT_TIME");
        for (MeasurementProperty p : propertiesToPrint) {
            out.write("\t" + p.toString());
        }
        out.write("\n");

        for (Measurement m : measurements.get(type)) {
            out.write(type + "\t" + BigDecimal.valueOf(m.getValue()).toPlainString() + "\t" + BigDecimal.valueOf(m.getTime()).toPlainString());
            for (MeasurementProperty prop : propertiesToPrint) {
                out.write("\t" + m.getProperty(prop));
            }
            out.write("\n");
        }

        out.flush();
    }

    public synchronized static void export(MeasurementType type,
                                           MeasurementProperty propertiesToPrint[],
                                           OutputStream output,
                                           boolean mergeAndExportTotal) throws IOException {

        if (mergeAndExportTotal) {
            /* Merge rewriting time measurements */
            long time = Measurements.getTotalTime(type);
            double value = Measurements.getTotal(type);

            Measurement m = new Measurement(type);
            m.setValue(value);
            m.setTime(time);

            Measurements.clear(type);
            Measurements.add(m);
        }

        export(type, propertiesToPrint, output);
    }

    public synchronized static void exportAdaptiveViews() throws IOException {
        // OutputStream out = new FileOutputStream("logs" + File.separator +
        // NodeInformation.localPID + "-statistics-" + ++exportCount + ".txt");
        //
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_SELECTED_VIEW, new
        // MeasurementProperty[]{MeasurementProperty.VIEW_NAME, MeasurementProperty.VIEW_DEFINITION,
        // MeasurementProperty.VIEW_SIZE}, out);
        //
        // Measurements.export(MeasurementType.QUERY_EXECUTION_TIME, new
        // MeasurementProperty[]{MeasurementProperty.QUERY_NAME, MeasurementProperty.LOGICAL_PLAN,
        // MeasurementProperty.QUERY_DEFINITION}, out);
        // Measurements.export(MeasurementType.QUERY_EXECUTION_TIME_TO_FIRST_RESULT, new
        // MeasurementProperty[]{MeasurementProperty.QUERY_NAME}, out);
        // Measurements.export(MeasurementType.QUERY_EXECUTION_RESULTS_NUMBER, new
        // MeasurementProperty[]{MeasurementProperty.QUERY_NAME}, out);
        //
        // Measurements.export(MeasurementType.VIEW_MATERIALIZATION_TIME, new
        // MeasurementProperty[]{MeasurementProperty.DOCUMENT_NAME}, out, true);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_CANDIDATE_VIEWS_ENUMERATION_EXECUTION_TIME,
        // new MeasurementProperty[]{}, out);
        // Measurements.export(MeasurementType.VIEW_SELECTION_NUMBER_OF_CANDIDATE_VIEWS, new
        // MeasurementProperty[]{}, out);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_ALGORITHM_EXECUTION_TIME, new
        // MeasurementProperty[]{}, out, false);
        // Measurements.export(MeasurementType.REWRITING_TIME, new MeasurementProperty[]{}, out,
        // true);
        // Measurements.export(MeasurementType.VIEW_SELECTION_CANDIDATE_VIEW_SIZE_ESTIMATION_TIME,
        // new MeasurementProperty[]{}, out, false);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_ALGORITHM, new
        // MeasurementProperty[]{MeasurementProperty.ALGORITHM_NAME}, out);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_ESTIMATED_WORKLOAD_SIZE, new
        // MeasurementProperty[]{}, out);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_SPACE_BUDGET, new
        // MeasurementProperty[]{}, out);
        // Measurements.export(MeasurementType.VIEW_SELECTION_SPACE_OCCUPATION, new
        // MeasurementProperty[]{}, out);
        //
        // Measurements.export(MeasurementType.VIEW_SELECTION_NUMBER_OF_SELECTED_VIEWS, new
        // MeasurementProperty[]{}, out);
        //
        // out.flush();
        // out.close();
        // Parameters.logger.info("Printed statistics in logs/*statistics.txt");
        //
        // Measurements.clear();
    }

    public synchronized static void export() throws IOException {
        // OutputStream out = new FileOutputStream("logs" + File.separator +
        // NodeInformation.localPID + "-statistics-" + ++exportCount + ".txt");
        //
        // for (Entry<MeasurementType, List<Measurement>> type : measurements.entrySet()) {
        // if (type.getValue().isEmpty())
        // continue;
        //
        // Set<MeasurementProperty> props = type.getValue().get(0).getProperties().keySet();
        // MeasurementProperty types[] = new MeasurementProperty[props.size()];
        //
        // int i = 0;
        // for (MeasurementProperty p : props) {
        // types[i] = p;
        // i++;
        // }
        //
        // Measurements.export(type.getKey(), types, out);
        // }
        //
        //
        // out.flush();
        // out.close();
        // Parameters.logger.info("Printed statistics in logs/*statistics.txt");
        //
        // Measurements.clear();
    }
}
