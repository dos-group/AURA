package de.tuberlin.aura.core.statistic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import net.jcip.annotations.ThreadSafe;

import org.apache.log4j.Logger;

import com.google.common.collect.ConcurrentHashMultiset;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;

/**
 * @author Christian Wuertz, Asterios Katsifodimos
 */
@ThreadSafe
public class MeasurementManager extends EventHandler {

    private static final Logger LOG = Logger.getLogger(MeasurementManager.class);

    private static String ROOT = null;

    // Events

    public static final String TASK_FINISHED = "TASK_FINISHED";

    private static HashMap<String, List<MeasurementManager>> exportListeners;

    private final EnumMap<MeasurementType, ConcurrentHashMultiset<Measurement>> measurements;

    // TODO: Make this configurable from the outside
    public String path;

    public String name;

    private MeasurementManager(String path, String name) {
        this.path = path;
        this.name = name;

        // Initialize the hash map from measurement types to the actual measurements.
        this.measurements = new EnumMap<>(MeasurementType.class);
        for (MeasurementType type : MeasurementType.values()) {
            this.measurements.put(type, ConcurrentHashMultiset.<Measurement>create());
        }
    }

    /**
     * Initialize the config of this class. This should be done by either the WorkloadManager or the
     * TaskManager.
     * <p/>
     * TODO: This should be done by the configuration framework in the future.
     * 
     * @param root
     */
    public static void setRoot(String root) {
        if (ROOT == null) {
            exportListeners = new HashMap<>();
            ROOT = root;
        }
    }

    public static void registerListener(String event, MeasurementManager measurementManager) {
        if (!exportListeners.containsKey(event)) {
            exportListeners.put(event, new LinkedList<MeasurementManager>());
        }

        exportListeners.get(event).add(measurementManager);
    }

    /**
     * Warning: DO NOT FIRE EVENTS WHEN YOU'RE NOT 100 % SURE THAT NOTHING WRITES INTO THE
     * MEASUREMENT MANAGERS ANYMORE!
     * <p/>
     * There is no synchronization to keep the performance penalty as low as possible.
     * 
     * @param event
     */
    public static void fireEvent(String event) {
        LOG.info("FireEvent: " + event);
        for (MeasurementManager manager : exportListeners.get(event)) {
            manager.export();
        }
    }

    public static MeasurementManager getInstance(String path, String name) {
        if (ROOT == null) {
            throw new IllegalArgumentException("The ROOT of the MeasurementManager wasn't initialized yet.");
        }

        return new MeasurementManager(path, name);
    }

    public MeasurementManager add(Measurement measurement) {
        this.measurements.get(measurement.getType()).add(measurement, 1);

        return this;
    }

    public void export() {

        Thread t = new Thread() {

            @Override
            public void run() {
                try {
                    LOG.info("Create " + ROOT + path);
                    File file = new File(ROOT + path);
                    file.getParentFile().mkdirs();
                    file.createNewFile();

                    // BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new
                    // GZIPOutputStream(new FileOutputStream(new File(path)))));
                    BufferedWriter out = new BufferedWriter(new FileWriter(file));

                    out.write(MeasurementManager.this.name);
                    out.write("\n");

                    for (MeasurementType type : MeasurementType.values()) {

                        out.write("TYPE: ");
                        out.write(type.toString());
                        out.write("\n");

                        boolean first = true;
                        SortedSet<Measurement> sortedSet = new TreeSet<>(new Comparator<Measurement>() {

                            @Override
                            public int compare(Measurement o1, Measurement o2) {
                                if (o1.timestamp == o2.timestamp) {
                                    return 0;
                                } else if (o1.timestamp < o2.timestamp) {
                                    return -1;
                                }

                                return 1;
                            }
                        });
                        sortedSet.addAll(measurements.get(type));
                        for (Measurement m : sortedSet) {
                            if (first) {
                                out.write(m.getHeader());
                                out.write("\n");

                                first = false;
                            }

                            out.write(m.toString());
                            out.write("\n");
                        }
                    }


                    out.close();
                } catch (IOException e) {
                    LOG.error("Writing measurements failed.", e);
                }
            }
        };

        t.start();
    }
}
