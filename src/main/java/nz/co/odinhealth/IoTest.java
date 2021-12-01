package nz.co.odinhealth;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class IoTest {

    private final Path directory;
    private final int iterations;
    private final byte[] text;
    private final int threads;
    private final BiConsumer<Integer, Integer> progressReporter;

    private final MetricRegistry metricRegistry;
    private final Timer deleteTimer;
    private final Timer closeTimer;
    private final Timer writeTimer;
    private final Timer newTimer;

    public IoTest(final Path directory, final long size, final int iterations, final int threads, final BiConsumer<Integer, Integer> progressReporter) {
        this.directory = directory;
        this.iterations = iterations;
        this.text = randomString(size).getBytes();
        this.threads = threads;
        this.progressReporter = progressReporter;

        this.newTimer = new Timer();
        this.writeTimer = new Timer();
        this.closeTimer = new Timer();
        this.deleteTimer = new Timer();

        this.metricRegistry = new MetricRegistry();
        metricRegistry.register("Create File", this.newTimer);
        metricRegistry.register("Write File", this.writeTimer);
        metricRegistry.register("Close File", this.closeTimer);
        metricRegistry.register("Delete File", this.deleteTimer);
    }

    public MetricRegistry getMetricRegistry() {
        return this.metricRegistry;
    }

    long runTest() throws Exception {
        final List<Path> files = LongStream.range(0, this.iterations).mapToObj(i -> String.format("file%d.txt", i)).map(fn -> this.directory.resolve(fn))
                .collect(Collectors.toList());

        final CountDownLatch writeLatch = new CountDownLatch(this.iterations);
        final CountDownLatch deleteLatch = new CountDownLatch(this.iterations);
        final ExecutorService executorService = Executors.newFixedThreadPool(this.threads);
        final long start = System.currentTimeMillis();
        files.forEach(path -> {
            executorService.execute(() -> {
                try {
                    this.writeFile(path, this.text);
                    writeLatch.countDown();
                    final long count = writeLatch.getCount();
                    if (count % 5 == 0) {
                        this.progressReporter.accept(this.iterations, this.iterations - (int)count);
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            });
        });
        writeLatch.await();
        files.forEach(path -> {
            executorService.execute(() -> {
                try {
                    this.deleteFile(path);
                    deleteLatch.countDown();
                } catch (final Exception e) {
                    e.printStackTrace();
                }
            });
        });
        deleteLatch.await();
        executorService.shutdown();
        return System.currentTimeMillis() - start;
    }

    void writeFile(final Path path, final byte[] text) throws Exception {
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

    }

    void deleteFile(final Path path) throws Exception {
        try (final Context timer4 = this.deleteTimer.time()) {
            Files.delete(path);
        }
    }

    private static String randomString(final long size) {
        return new Random().ints(32, 127)
                .limit(size)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

}
