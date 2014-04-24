package de.tuberlin.aura.core.statistic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import net.jcip.annotations.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ConcurrentHashMultiset;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;

@ThreadSafe
public class MeasurementManager extends EventHandler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MeasurementManager.class);

    private static String ROOT = null;

    // Events

    public static final String TASK_FINISHED = "TASK_FINISHED";

    private static HashMap<String, List<MeasurementManager>> exportListeners;

    private final EnumMap<MeasurementType, ConcurrentHashMultiset<Measurement>> measurements;

    public String path;  // TODO: Make this configurable from the outside

    public String name;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private MeasurementManager(String path, String name) {
        this.path = path;
        this.name = name;

        // Initialize the hash map from measurement types to the actual measurements.
        this.measurements = new EnumMap<>(MeasurementType.class);
        for (MeasurementType type : MeasurementType.values()) {
            this.measurements.put(type, ConcurrentHashMultiset.<Measurement>create());
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

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
        LOG.trace("FireEvent: " + event);
        List<MeasurementManager> managers = exportListeners.get(event);
        if (managers != null) {
            for (MeasurementManager manager : managers) {
                manager.export();
            }

            exportListeners.remove(event);
        } else {
            LOG.error("The same event ({}) was fired twice. Probably the same job was executed twice and the first didn't finished yet", event);
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
                    LOG.trace("Create " + ROOT + path);
                    File file = new File(ROOT + path);
                    file.getParentFile().mkdirs();
                    file.createNewFile();

                    // BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new
                    // GZIPOutputStream(new FileOutputStream(new File(path)))));
                    BufferedWriter out = new BufferedWriter(new FileWriter(file, true));

                    out.write("JOB EXECUTION\n");
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
                                    return Math.abs(o1.hashCode() - o2.hashCode());
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
