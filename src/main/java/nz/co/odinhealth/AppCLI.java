package nz.co.odinhealth;

import com.codahale.metrics.ConsoleReporter;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class AppCLI {

    public static void main(final String[] args) {

        final CommandLineParser parser = new DefaultParser();
        final Options options = new Options();
        options.addRequiredOption("d",  "dir", true, "Directory path to write");
        options.addOption("s",  "size", true, "Number of bytes to write in each file. Default to '104857600', i.e. 100MB");
        options.addOption("l",  "loops", true, "Number of loops. Default to '100'");
        options.addOption("t", "threads", true, "Number of threads to write in parallel, default to available CPU cores");

        try {
            final CommandLine line = parser.parse(options, args);
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final Path directory = Paths.get(line.getOptionValue("d"));
            if (Files.notExists(directory) || !Files.isDirectory(directory)) {
                throw new RuntimeException(String.format("Directory path '%s' does not exist or is not a directory", directory.toAbsolutePath()));
            }
            System.out.printf("Writing to directory: '%s'%n", directory.toAbsolutePath());

            final long size = line.hasOption("s") ? Long.parseLong(line.getOptionValue("s")) : 104857600;
            System.out.printf("File Size = '%s' bytes%n", size);

            final int iterations = line.hasOption("l") ? Integer.parseInt(line.getOptionValue("l")) : 100;
            System.out.printf("Loops = %s%n", iterations);

            final int threads = line.hasOption("t") ? Integer.parseInt(line.getOptionValue("t")) : Runtime.getRuntime().availableProcessors();
            System.out.printf("Threads = %s%n", threads);

            checkAvailableSpace(directory, size * iterations);

            final IoTest test = new IoTest(directory, size, iterations, threads, AppCLI::printProgress);
            final long duration = test.runTest();
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(test.getMetricRegistry()).build();
            // report will do 1 report before closing
            System.out.println();
            reporter.close();
            System.out.printf("%nTest completed in %.1f seconds%n", duration / (double)1000);
            System.out.println();
            printSystemSpecs(mBeanServer, directory);

        } catch (final Exception e) {
            System.out.println();
            System.err.println("Error: " + e.getMessage());
            System.out.println();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar file-test.jar", options, true);
        }
    }

    private static void printSystemSpecs(final MBeanServer server, final Path directory) throws Exception {
        System.out.println("====== System Specs ======");
        System.out.println("CPU cores: " + Runtime.getRuntime().availableProcessors());
        Object memoryTotal = server.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
        Object memoryFree = server.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
        System.out.printf("Memory total: %s GB%n", Long.parseLong(memoryTotal.toString()) / 1024 / 1024 / 1024);
        System.out.printf("Memory free: %.2f GB%n", Long.parseLong(memoryFree.toString()) / (double)(1024 * 1024 * 1024));
        final FileStore fileStore = Files.getFileStore(directory);
        final long diskTotal = fileStore.getTotalSpace() / 1024 / 1024 / 1024;
        final double usableTotal = fileStore.getUsableSpace() / (double)(1024 * 1024 * 1024);
        System.out.printf("Disk space of '%s' (type=%s)%n", fileStore.name(), fileStore.type());
        System.out.printf("Disk space total: %s GB%n", diskTotal);
        System.out.printf("Disk space available: %.2f GB%n", usableTotal);
    }

    static void checkAvailableSpace(final Path directory, final long expected) throws Exception{
        final FileStore fileStore = Files.getFileStore(directory);
        if (fileStore.getUsableSpace() < expected) {
            throw new RuntimeException(String.format("Not enough disk space to run this test. It requires %.2f GB available spaces in '%s'. Please reduce your -s (size) or -l (loops) parameters", expected / (double) (1024 * 1024 * 1024), fileStore.name()));
        }
    }

    static void printProgress(final int total, final int current) {
        final int percent = current * 100 / total;
        System.out.print('\r' + String.format(" %d%%", percent) + String.format(" [%d/%d]", current, total));
    }

}
