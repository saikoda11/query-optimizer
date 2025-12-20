package edu.cmu.cs.db.calcite_app.app;

import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class InputDirectoryProcessorTest {
    @Test
    @DisplayName("Test readOrder()")
    public void testReadOrder() {
        AtomicInteger order = new AtomicInteger(-1);
        assertDoesNotThrow(() -> order.set(InputDirectoryProcessor.readOrder(Path.of("../input/queries/q1.order"))));
        assertEquals(1, order.get());
    }

    @Test
    public void testProcessDirThrowsWhenDirDoesNotExist() {
        assertThrows(
                IllegalArgumentException.class,
                () -> InputDirectoryProcessor.processDir("../input/non-existent-dir")
        );
    }

    @Test
    public void testProcessDirThrowsWhenDirIsFile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> InputDirectoryProcessor.processDir("../input/queries/capybara1.sql")
        );
    }


    @Test
    public void testProcessDirFilesValidity() {
        try {
            List<Path> paths = InputDirectoryProcessor.processDir("../input/queries/");

            assert !paths.isEmpty();
            assertTrue(paths.stream().allMatch(p -> {
                File file = p.toFile();
                return file.exists() && file.getName().endsWith(".sql");
            }));

        } catch (IllegalArgumentException exception) {
            fail("Unexpected throw: " + exception.getMessage());
        }
    }

    @Test
    public void testProcessDirFilesOrdering() {
        try {
            List<Path> paths = InputDirectoryProcessor.processDir("../input/queries/");
            checkOrder(paths);
        } catch (IllegalArgumentException exception) {
            fail("Unexpected throw: " + exception.getMessage());
        }
    }


    private void checkOrder(List<Path> orderedSqlPaths) {
        var orderList = orderedSqlPaths.stream()
                .map(p -> {
                    String parent = p.getParent().toString();
                    String filename = p.toFile().getName();
                    int lastDotIndex = filename.lastIndexOf('.');
                    String fileNameWithoutExtension = filename.substring(0, lastDotIndex);
                    Path orderFilePath = Path.of(parent + File.separator + fileNameWithoutExtension + ".order");
                    return InputDirectoryProcessor.readOrder(orderFilePath);
                }).toList();
        assertEquals(orderedSqlPaths.size(), orderList.size());
        for (int i = 0; i < orderList.size() - 1; i++) {
            assertTrue(orderList.get(i) <= orderList.get(i + 1));
        }
    }

}
