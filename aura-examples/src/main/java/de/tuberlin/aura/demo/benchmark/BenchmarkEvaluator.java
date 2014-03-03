package de.tuberlin.aura.demo.benchmark;

import de.tuberlin.aura.core.task.common.MedianHelper;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;

/**
 * Created by teots on 2/27/14.
 */
public class BenchmarkEvaluator {
    private String path;

    public BenchmarkEvaluator(String path) {
        this.path = path;
    }

    public void evaluate() {
        DecimalFormat format = new DecimalFormat("000");

        double avgSum = 0.0d;
        double minSum = 0.0d;
        double maxSum = 0.0d;
        long medSum = 0l;
        int results = 0;

        ArrayList<Long> queuingTimes = new ArrayList<>();
        ArrayList<Integer> queueSize = new ArrayList<>();

        File rootFolder = new File(this.path);
        for (File folder : rootFolder.listFiles()) {
            for (File file : folder.listFiles()) {

                System.out.println(folder.getName());

                try {
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        if (line.contains("RESULTS")) {
                            String[] tokens = line.split(",");

                            avgSum += Double.parseDouble(tokens[1]);
                            minSum += Double.parseDouble(tokens[2]);
                            maxSum += Double.parseDouble(tokens[3]);
                            medSum += Long.parseLong(tokens[4]);

                            ++results;

                            System.out.println(line);

                        } else if (line.toLowerCase().contains("exception")) {
                            System.out.println(line);
                        } else if (line.contains("TIME_IN_QUEUE")) {
                            String[] tokens = line.split(":");
                            queuingTimes.add(Long.parseLong(tokens[1]));
                            queueSize.add(Integer.parseInt(tokens[2]));
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

        long minQueueLatency = Long.MAX_VALUE;
        long maxQueueLatency = Long.MIN_VALUE;
        long queueLatencySum = 0l;

        for (long latency : queuingTimes) {
            queueLatencySum += latency;

            if (latency < minQueueLatency) {
                minQueueLatency = latency;
            }

            if (latency > maxQueueLatency) {
                maxQueueLatency = latency;
            }
        }

        long queueSizeSum = 0;
        for (int size : queueSize) {
            queueSizeSum += size;
        }

        System.out.println("Avg Latency: " + Double.toString(avgSum / (double) results) + " ms");
        System.out.println("Min Latency: " + Double.toString(minSum / (double) results) + " ms");
        System.out.println("Max Latency: " + Double.toString(maxSum / (double) results) + " ms");
        System.out.println("Median Latency: " + Double.toString((double) medSum / (double) results) + " ms");
        System.out.println("Avg Queue Latency: " + Double.toString(queueLatencySum / (double) queuingTimes.size()) + " ms");
        System.out.println("Min Queue Latency: " + Long.toString(minQueueLatency) + " ms");
        System.out.println("Max Queue Latency: " + Long.toString(maxQueueLatency) + " ms");
        System.out.println("Median Queue Latency: " + MedianHelper.findMedian(queuingTimes) + " ms");
        System.out.println("Avg Queue Size: " + Double.toString((double) queueSizeSum / (double) queueSize.size()) + " ms");
    }

    public static void main(String[] args) {
        BenchmarkEvaluator evaluator = new BenchmarkEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
