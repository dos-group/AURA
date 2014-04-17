package de.tuberlin.aura.demo.benchmark;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by teots on 2/27/14.
 */
public class LogEvaluator {

    private String path;

    public LogEvaluator(String path) {
        this.path = path;
    }

    public void evaluate() {
        List<String> mappers = new LinkedList<>();
        List<String> reducers = new LinkedList<>();

        File rootFolder = new File(this.path);
        for (File nodeFolder : rootFolder.listFiles()) {
            for (File file : nodeFolder.listFiles()) {
                // Read logs.
                if (file.isFile()) {
                    checkForExceptions(file);
                } else {
                    for (File measurementFile : file.listFiles()) {
                        // Read the mapper logs.
                        checkForExceptions(measurementFile);

                        if (measurementFile.getAbsolutePath().contains("Mapper")) {
                            mappers.add(measurementFile.getAbsolutePath());
                        } else if (measurementFile.getAbsolutePath().contains("Reducer")) {
                            reducers.add(measurementFile.getAbsolutePath());
                        }
                    }
                }
            }
        }

        Collections.sort(mappers);
        Collections.sort(reducers);

        System.out.println("Mappers:");
        for (String mapper : mappers) {
            System.out.println(mapper);
        }

        System.out.println("Reducers:");
        for (String reducer : reducers) {
            System.out.println(reducer);
        }

        System.out.println("Mappers: " + Integer.toString(mappers.size()));
        System.out.println("Reducers: " + Integer.toString(reducers.size()));
    }

    private void checkForExceptions(File file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.contains("Exception")) {
                    System.out.print(file.getPath() + " -> ");
                    System.out.println(line);
                }
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        LogEvaluator evaluator = new LogEvaluator("/home/teots/Desktop/logs");
        evaluator.evaluate();
    }
}
