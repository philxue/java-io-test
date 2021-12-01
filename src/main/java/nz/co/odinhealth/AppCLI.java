package nz.co.odinhealth;

import com.codahale.metrics.ConsoleReporter;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Security;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class AppCLI {

    public static void main(final String[] args) throws Exception {

        final CommandLineParser parser = new DefaultParser();
        final Options options = new Options();
        options.addRequiredOption("d",  "dir", true, "Directory path to write");
        options.addOption("s",  "size", true, "Number of bytes to write in each file. Default to '104857600', i.e. 100MB");
        options.addOption("l",  "loops", true, "Number of loops. Default to '300'");
        options.addOption("t", "threads", true, "Number of threads to write in parallel, default to available CPU cores");

        try {
            final CommandLine line = parser.parse(options, args);
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            final Path directory = Paths.get(line.getOptionValue("d"));
            if (Files.notExists(directory) || !Files.isDirectory(directory)) {
                throw new RuntimeException(String.format("Directory path '%s' does not exist or is not a directory", directory.toAbsolutePath()));
            }
            System.out.println(String.format("Writing to directory: '%s'", directory.toAbsolutePath()));

            final long size = line.hasOption("s") ? Long.parseLong(line.getOptionValue("s")) : 104857600;
            System.out.println(String.format("File Size = '%s' bytes", size));

            final int iterations = line.hasOption("l") ? Integer.parseInt(line.getOptionValue("l")) : 300;
            System.out.println(String.format("Loops = %s", iterations));

            final int threads = line.hasOption("t") ? Integer.parseInt(line.getOptionValue("t")) : Runtime.getRuntime().availableProcessors();;
            System.out.println(String.format("Threads = %s", threads));

            checkAvailableSpace(mBeanServer, directory, size * iterations);

            final IoTest test = new IoTest(directory, size, iterations, threads, AppCLI::printProgress);
            final long duration = test.runTest();
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(test.getMetricRegistry()).build();
            // report will do 1 report before closing
            System.out.println("");
            reporter.close();
            System.out.println(String.format("\nTest completed in %.1f seconds", duration / (double)1000));
            System.out.println("");
            printSystemSpecs(mBeanServer, directory);

        } catch (final Exception e) {
            System.out.println("");
            System.err.println("Error: " + e.getMessage());
            System.out.println("");
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar file-test.jar", options, true);
        }
    }

    private static void printSystemSpecs(final MBeanServer server, final Path directory) throws Exception {
        System.out.println("====== System Specs ======");
        System.out.println("CPU Cores: " + Runtime.getRuntime().availableProcessors());
        Object memoryTotal = server.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
        Object memoryFree = server.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
        System.out.println(String.format("Memory Total: %s GB", Long.parseLong(memoryTotal.toString()) / 1024 / 1024 / 1024));
        System.out.println(String.format("Memory Free: %.2f GB", Long.parseLong(memoryFree.toString()) / (double)(1024 * 1024 * 1024)));
        final FileStore fileStore = Files.getFileStore(directory);
        final long diskTotal = fileStore.getTotalSpace() / 1024 / 1024 / 1024;
        final double usableTotal = fileStore.getUsableSpace() / (double)(1024 * 1024 * 1024);
        System.out.println(String.format("Diskspace of '%s' (type=%s)", fileStore.name(), fileStore.type()));
        System.out.println(String.format("Diskspace Total: %s GB", diskTotal));
        System.out.println(String.format("Diskspace Available: %.2f GB", usableTotal));
    }

    static void checkAvailableSpace(final MBeanServer server, final Path directory, final long expected) throws Exception{
        final FileStore fileStore = Files.getFileStore(directory);
        if (fileStore.getUsableSpace() < expected) {
            throw new RuntimeException(String.format("Not enough disk space to run this test. It requires %.2f GB available spaces in '%s'. Please reduce your -s (size) or -l (loops) parameters", expected / (double) (1024 * 1024 * 1024), fileStore.name()));
        }
    }

    static void printProgress(final int total, final int current) {
        final StringBuilder string = new StringBuilder(32);
        final int percent = (int) (current * 100 / total);
        string.append('\r').append(String.format(" %d%%", percent)).append(String.format(" [%d/%d]", current, total));
        System.out.print(string);
    }

}
