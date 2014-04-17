package de.tuberlin.aura.demo.benchmark;

import java.io.*;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import de.tuberlin.aura.core.task.common.MedianHelper;

/**
 * Created by teots on 2/27/14.
 */
public class MapReduceEvaluator {

    private String path;

    public MapReduceEvaluator(String path) {
        this.path = path;
    }

    public void evaluate() {
        DecimalFormat format = new DecimalFormat("0.00");

        Map<Long, Double> accThroughput = new TreeMap<>();

        File rootFolder = new File(this.path);
        for (File nodeFolder : rootFolder.listFiles()) {

            File tmNodeFolder = new File(nodeFolder.getAbsolutePath() + "/tm");
            if (tmNodeFolder.isDirectory()) {

                for (File file : tmNodeFolder.listFiles()) {
                    // Read the reducer logs.
                    if (file.getAbsolutePath().contains("Reducer")) {
                        try {
                            BufferedReader br = new BufferedReader(new FileReader(file));
                            String line = null;
                            while ((line = br.readLine()) != null) {
                                if (line.contains("Throughput")) {
                                    String[] tokens = line.split("\\t");
                                    long timeslot = Long.parseLong(tokens[0]);

                                    tokens = tokens[2].split(" ");
                                    double throughput = Double.parseDouble(tokens[0]);

                                    if (!accThroughput.containsKey(timeslot)) {
                                        accThroughput.put(timeslot, throughput);
                                    } else {
                                        accThroughput.put(timeslot, accThroughput.get(timeslot) + throughput);
                                    }
                                }
                            }

                            br.close();
                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        long lastSlot = -1l;
        boolean first = true;
        List<Double> throughputs = new LinkedList<>();
        for (Map.Entry<Long, Double> entry : accThroughput.entrySet()) {

            // Detect the last measurement of a job.
            if (lastSlot == -1l || (entry.getKey() - lastSlot) > 1) {
                if (!throughputs.isEmpty()) {
                    throughputs.remove(throughputs.size() - 1);

                    // Since the last measurement of the last job was removed, this measurement will
                    // be the first of a new job and thus removed.
                    first = true;
                }

                System.out.println("Probably a new Job...");
            }

            if (first) {
                first = false;
            } else {
                throughputs.add(entry.getValue());
            }

            lastSlot = entry.getKey();
            System.out.println("Timeslot: " + Long.toString(entry.getKey()) + " -> " + format.format(entry.getValue()) + " Mbit/s");
        }

        // Remove the last measurement of the last job.
        throughputs.remove(throughputs.size() - 1);

        double minThroughput = Double.MAX_VALUE;
        double maxThroughput = Double.MIN_VALUE;
        double avgThroughput = 0.0d;
        double mdnThroughput = MedianHelper.findMedianDouble(throughputs);

        for (Double throughput : throughputs) {
            if (throughput < minThroughput) {
                minThroughput = throughput;
            }

            if (throughput > maxThroughput) {
                maxThroughput = throughput;
            }

            avgThroughput += throughput;
        }

        avgThroughput /= (double) throughputs.size();

        System.out.println();
        System.out.println("Overall statistics (without the first and last measurement in each job execution)");
        System.out.println("Min throughput: " + format.format(minThroughput));
        System.out.println("Max throughput: " + format.format(maxThroughput));
        System.out.println("Avg throughput: " + format.format(avgThroughput));
        System.out.println("Mdn throughput: " + format.format(mdnThroughput));
    }

    public static void main(String[] args) {
        MapReduceEvaluator evaluator = new MapReduceEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
