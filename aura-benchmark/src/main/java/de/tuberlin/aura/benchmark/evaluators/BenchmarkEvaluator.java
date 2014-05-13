package de.tuberlin.aura.benchmark.evaluators;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;

import de.tuberlin.aura.core.measurement.MedianHelper;

/**
 *
 */
public class BenchmarkEvaluator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private String path;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public BenchmarkEvaluator(String path) {
        this.path = path;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void evaluate() {
        DecimalFormat format = new DecimalFormat("000");

        double avgSum = 0.0d;
        long minSum = 0l;
        long maxSum = 0l;
        long medSum = 0l;
        int results = 0;

        ArrayList<Double> queuingTimes = new ArrayList<>();
        ArrayList<Double> queueSize = new ArrayList<>();

        File rootFolder = new File(this.path);
        for (File folder : rootFolder.listFiles()) {
            for (File file : folder.listFiles()) {

                System.out.println(folder.getName());

                try {
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if (line.contains("RESULTS")) {
                            String[] tokens = line.split("\\|");

                            avgSum += Double.parseDouble(tokens[1]);
                            minSum += Long.parseLong(tokens[2]);
                            maxSum += Long.parseLong(tokens[3]);
                            medSum += Long.parseLong(tokens[4]);

                            ++results;

                            System.out.println(line);

                        } else if (line.toLowerCase().contains("exception")) {
                            System.out.println(line);
                        } else if (line.contains("TIME_IN_QUEUE")) {
                            String[] tokens = line.split("\\|");
                            queuingTimes.add(Double.parseDouble(tokens[1]));
                            queueSize.add(Double.parseDouble(tokens[2]));
                        }
                    }

                    br.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                System.out.println();
            }
        }

        double minQueueLatency = Long.MAX_VALUE;
        double maxQueueLatency = Long.MIN_VALUE;
        double queueLatencySum = 0l;

        for (double latency : queuingTimes) {
            queueLatencySum += latency;

            if (latency < minQueueLatency) {
                minQueueLatency = latency;
            }

            if (latency > maxQueueLatency) {
                maxQueueLatency = latency;
            }
        }

        double queueSizeSum = 0;
        for (double size : queueSize) {
            queueSizeSum += size;
        }

        System.out.println("Avg Latency: " + Double.toString(avgSum / (double) results) + " ms");
        System.out.println("Min Latency: " + Double.toString(minSum / (double) results) + " ms");
        System.out.println("Max Latency: " + Double.toString(maxSum / (double) results) + " ms");
        System.out.println("Median Latency: " + Double.toString((double) medSum / (double) results) + " ms");
        System.out.println("Avg Queue Latency: " + Double.toString(queueLatencySum / (double) queuingTimes.size()) + " ms");
        System.out.println("Min Queue Latency: " + Double.toString(minQueueLatency) + " ms");
        System.out.println("Max Queue Latency: " + Double.toString(maxQueueLatency) + " ms");
        System.out.println("Median Queue Latency: " + MedianHelper.findMedianDouble(queuingTimes) + " ms");
        System.out.println("Avg Queue Size: " + Double.toString((double) queueSizeSum / (double) queueSize.size()));
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {
        BenchmarkEvaluator evaluator = new BenchmarkEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
