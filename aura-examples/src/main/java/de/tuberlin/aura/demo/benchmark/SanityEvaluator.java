package de.tuberlin.aura.demo.benchmark;

import java.io.*;

/**
 * Created by teots on 2/27/14.
 */
public class SanityEvaluator {

    private static final int EXECUTION_UNITS = 8;

    private String path;

    public SanityEvaluator(String path) {
        this.path = path;
    }

    public void evaluate() {

        int sumSources = 0;
        int sumSinks = 0;

        File rootFolder = new File(this.path);
        for (File nodeFolder : rootFolder.listFiles()) {
            for (File file : nodeFolder.listFiles()) {
                // Read logs.
                if (file.isFile()) {
                    checkForExceptions(file);
                } else {
                    // Read the content of folders
                    for (File measurementFile : file.listFiles()) {
                        try {
                            BufferedReader br = new BufferedReader(new FileReader(measurementFile));
                            String line = null;
                            while ((line = br.readLine()) != null) {
                                if (line.contains("SOURCE")) {
                                    String[] tokens = line.split("\\t");
                                    sumSources += Integer.parseInt(tokens[2]);
                                } else if (line.contains("SINK")) {
                                    String[] tokens = line.split("\\t");
                                    sumSinks += Integer.parseInt(tokens[2]);
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

        System.out.println("Buffer Diff: " + Integer.toString(sumSources - sumSinks));
    }

    private void checkForExceptions(File file) {

        int running = 0;
        int finished = 0;

        int sink = 0;

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.toLowerCase().contains("exception") || line.toLowerCase().contains("ERROR") || line.contains("WARN")) {
                    System.out.print(file.getPath() + " -> ");
                    System.out.println(line);
                } else if (line.contains("TASK_STATE_RUNNING  [TASK_TRANSITION_RUN]")) {
                    ++running;
                } else if (line.contains("TASK_STATE_FINISHED  [TASK_TRANSITION_FINISH]")) {
                    ++finished;
                }
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (running != finished) {
            System.out.println(file);
            System.out.println("Running: " + Integer.toString(running) + " Finished: " + Integer.toString(finished));
        }
    }

    public static void main(String[] args) {
        SanityEvaluator evaluator = new SanityEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
