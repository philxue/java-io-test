package nz.co.odinhealth;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class App {

    private final Path directory;
    private final int iterations;
    private final byte[] text;
    private final Timer deleteTimer;
    private final Timer closeTimer;
    private final Timer writeTimer;
    private final Timer newTimer;

    public App(final Path directory, final int iterations, final byte[] text, final MetricRegistry metricRegistry) {
        this.directory = directory;
        this.iterations = iterations;
        this.text = text;
        this.newTimer = new Timer();
        this.writeTimer = new Timer();
        this.closeTimer = new Timer();
        this.deleteTimer = new Timer();
        metricRegistry.register("Create File", this.newTimer);
        metricRegistry.register("Write File", this.writeTimer);
        metricRegistry.register("Close File", this.closeTimer);
        metricRegistry.register("Delete File", this.deleteTimer);
    }

    long doTest() throws Exception {

        final List<Path> files = IntStream.range(0, this.iterations).mapToObj(i -> String.format("file%d.txt", i)).map(fn -> this.directory.resolve(fn))
                .collect(Collectors.toList());

        final CountDownLatch latch = new CountDownLatch(this.iterations);
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final long start = System.currentTimeMillis();
        files.forEach(path -> {
            executorService.execute(() -> {
                try {
                    this.testFile(path, this.text);
                    latch.countDown();
                    final long count = latch.getCount();
                    if (count % 10 == 0) {
                        printProgress(start, this.iterations, this.iterations - count);
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            });
        });
        latch.await();
        executorService.shutdown();
        return System.currentTimeMillis() - start;
    }

    void testFile(final Path path, final byte[] text) throws Exception {
        OutputStream outputStream = null;
        try (final Context timer = this.newTimer.time()) {
            outputStream = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        }

        try (final Context timer = this.writeTimer.time()) {
            outputStream.write(text);
            outputStream.flush();
        }

        try (final Context timer = this.closeTimer.time()) {
            outputStream.close();
        }

        try (final Context timer4 = this.deleteTimer.time()) {
            Files.delete(path);
        }
    }

    private static String randomString(final int size) {
        return new Random().ints(32, 127)
                .limit(size)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private static void printProgress(final long startTime, final long total, final long current) {
//        final long eta = current == 0 ? 0 :
//                (total - current) * (System.currentTimeMillis() - startTime) / current;
//
//        final String etaHms = current == 0 ? "N/A" :
//                String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
//                        TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
//                        TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

        final StringBuilder string = new StringBuilder(140);
        final int percent = (int) (current * 100 / total);
        string
                .append('\r')
                .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
                .append(String.format(" %d%% [", percent))
                .append(String.join("", Collections.nCopies(percent, "=")))
                .append('>')
                .append(String.join("", Collections.nCopies(100 - percent, " ")))
                .append(']')
                .append(String.join("", Collections.nCopies(current == 0 ? (int) (Math.log10(total)) : (int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
//                .append(String.format(" %d/%d, ETA: %s", current, total, etaHms));
                .append(String.format(" %d/%d", current, total));

        System.out.print(string);
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: file-test <directory> <text-size> <iterations>");
            System.out.println("Example: java -jar file-test-1.0.4.jar D:\\Test 102400 10000");
            System.exit(-1);
        }

        final Path path = Paths.get(args[0]);
        if (Files.notExists(path) || !Files.isDirectory(path)) {
            System.err.println(String.format("Path '%s' does not exist or is not a directory", path.toAbsolutePath()));
            System.exit(-1);
        }
        final byte[] text = randomString(Integer.parseInt(args[1])).getBytes();
        final int iterations = Integer.parseInt(args[2]);

        final MetricRegistry metricRegistry = new MetricRegistry();

        final App app = new App(path, iterations, text, metricRegistry);
        final long duration = app.doTest();

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).build();
        reporter.start(5, TimeUnit.MICROSECONDS);
        reporter.report();
        reporter.close();
        System.out.println(String.format("Test completed in %s seconds", duration / 1000));
    }

}
