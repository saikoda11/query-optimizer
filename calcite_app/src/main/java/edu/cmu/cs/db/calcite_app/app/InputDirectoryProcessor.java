package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

public class InputDirectoryProcessor {
    public static List<Path> processDir(String queriesDirStr) {
        Path queriesDir = Paths.get(queriesDirStr).toAbsolutePath();
        File file = queriesDir.toFile();
        if (!file.exists() || !file.isDirectory()) {
            throw new IllegalArgumentException("does not exist or is not a directory");
        }
        try (Stream<Path> paths = Files.walk(queriesDir)) {
            return paths
                    .filter(p -> p.getFileName().toString().endsWith(".order"))
                    .sorted((p1, p2) -> {
                        int order1 = readOrder(p1);
                        int order2 = readOrder(p2);
                        if (order1 == order2) {
                            return p1.toFile().getName().compareTo(p2.toFile().getName());
                        }
                        return order1 - order2;
                    })
                    .map(p -> {
                        String parent = p.getParent().toString();
                        String filename = p.toFile().getName();
                        int lastDotIndex = filename.lastIndexOf('.');
                        String fileNameWithoutExtension = filename.substring(0, lastDotIndex);
                        return Path.of(parent + File.separator + fileNameWithoutExtension + ".sql");
                    })
                    .toList();
        } catch (IOException ignored) {
            return Collections.emptyList();
        }
    }

    public static int readOrder(Path p) {
        if (!p.toFile().getName().endsWith(".order")) {
            throw new IllegalArgumentException("file is not a valid order file");
        }
        try (Scanner scanner = new Scanner(p)) {
            return scanner.nextInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
