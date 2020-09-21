package io.airbyte.singer_base;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.airbyte.commons.io.IOs;
import io.airbyte.config.Schema;
import io.airbyte.commons.json.Jsons;
import io.airbyte.singer.SingerCatalog;
import io.airbyte.workers.protocols.singer.DefaultSingerStreamFactory;
import io.airbyte.workers.protocols.singer.SingerCatalogConverters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SingerBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingerBase.class);
  private static final String SINGER_EXECUTABLE = "SINGER_EXECUTABLE";

  private enum Command {
    SPEC, CHECK, DISCOVER, READ, WRITE
  }

  private enum Inputs {
    CONFIG, SCHEMA, STATE
  }

  private static final OptionGroup commandGroup = new OptionGroup();

  static {
    commandGroup.setRequired(true);
    commandGroup.addOption(Option.builder()
        .longOpt(Command.SPEC.toString().toLowerCase())
        .desc("outputs the json configuration specification")
        .build());
    commandGroup.addOption(Option.builder()
        .longOpt(Command.CHECK.toString().toLowerCase())
        .desc("checks the config can be used to connect")
        .build());
    commandGroup.addOption(Option.builder()
        .longOpt(Command.DISCOVER.toString().toLowerCase())
        .desc("outputs a catalog describing the source's schema")
        .build());
    commandGroup.addOption(Option.builder()
        .longOpt(Command.READ.toString().toLowerCase())
        .desc("reads the source and outputs messages to STDOUT")
        .build());
    commandGroup.addOption(Option.builder()
        .longOpt(Command.WRITE.toString().toLowerCase())
        .desc("writes messages from STDIN to the integration")
        .build());
  }

  public void run(final String[] args) throws IOException {
    final String singerExecutable = System.getenv(SINGER_EXECUTABLE);

    final Command command = parseCommand(args);
    final Map<String, String> options = parseOptions(args, command);
    final Map<String, String> newOptions = transformInput(options, singerExecutable); // mapping from airbyte to singer structs, field mapping, etc.
    final String newArgs = toCli(newOptions);

    Preconditions.checkNotNull(singerExecutable, SINGER_EXECUTABLE + " environment variable cannot be null.");

    final String cmd = singerExecutable + newArgs;
    final ProcessBuilder processBuilder = new ProcessBuilder(cmd);
    final Process process = processBuilder.start();

    transformOutput(process.getInputStream(), command); // mapping from singer structs back to airbyte and then pipe to stdout.
  }

  private static String toCli(Map<String, String> newArgs) {
    return newArgs.entrySet().stream().map(entry -> "--" + entry.getKey() + (entry.getValue() != null ? " " + entry.getValue() : ""))
        .collect(Collectors.joining(" "));
  }

  // no-op for now.
  private Map<String, String> transformInput(Map<String, String> parsedArgs, String singerExecutable) throws IOException {
    final Map<String, String> outputArgs = new HashMap<>();
    for (Map.Entry<String, String> entry : parsedArgs.entrySet()) {
      switch (Inputs.valueOf(entry.getKey())) {
        case SCHEMA:
          final SingerCatalog singerCatalog = runDiscover(singerExecutable, parsedArgs);
          final Schema discoveredCatalog = Jsons.deserialize(IOs.readFile(Path.of(entry.getValue())), Schema.class);
          final SingerCatalog catalog = SingerCatalogConverters.applySchemaToDiscoveredCatalog(singerCatalog, discoveredCatalog);
          final Path processedCatalogPath = Path.of("processed_catalog.json");
          IOs.writeFile(processedCatalogPath, Jsons.serialize(catalog));
          outputArgs.put("properties", processedCatalogPath.toString());
        case CONFIG: // todo (cgardens) - allow field mapping.
        case STATE:
        default:
          LOGGER.info("Unrecognized input {}. Not transforming.", entry.getKey());
          outputArgs.put(entry.getKey(), entry.getValue());
      }
    }
    return outputArgs;
  }

  private SingerCatalog runDiscover(String singerExecutable, Map<String, String> options) throws IOException {
    final Set<String> commandSet = Arrays.stream(Command.values()).map(cmd -> cmd.toString().toLowerCase()).collect(Collectors.toSet());
    final Map<String, String> discoverOptions =
        options.entrySet()
            .stream()
            .filter(entry -> !commandSet.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final String cmd = singerExecutable + toCli(discoverOptions);
    final ProcessBuilder processBuilder = new ProcessBuilder(cmd);
    final Process process = processBuilder.start();

    return getCatalogFromDiscoverInputStream(process.getInputStream()).orElseThrow(() -> new RuntimeException("No catalog found"));
  }

  // no-op for now.
  private void transformOutput(InputStream inputStream, Command command) throws IOException {
    switch (command) {
      case DISCOVER:
        convertCatalogStreamToSchemaStream(inputStream);
        break;
      case SPEC:
      case CHECK:
      case READ:
      case WRITE:
        toStdout(inputStream);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized command: " + command);
    }
  }

  private void toStdout(InputStream inputStream) throws IOException {
    int ch;
    while ((ch = inputStream.read()) != -1) {
      System.out.write(ch);
    }
    System.out.flush();
  }

  private Optional<SingerCatalog> getCatalogFromDiscoverInputStream(InputStream inputStream) {
    final Iterator<SingerCatalog> iterator = discoverInputStreamToIterator(inputStream);
    if (iterator.hasNext()) {
      return Optional.of(iterator.next());
    }
    return Optional.empty();
  }

  private Iterator<SingerCatalog> discoverInputStreamToIterator(InputStream inputStream) {
    final DefaultSingerStreamFactory<SingerCatalog> streamFactory = DefaultSingerStreamFactory.catalog();
    return streamFactory.create(IOs.newBufferedReader(inputStream)).iterator();
  }

  private void convertCatalogStreamToSchemaStream(InputStream inputStream) throws IOException {
    final Iterator<SingerCatalog> iterator = discoverInputStreamToIterator(inputStream);
    while (iterator.hasNext()) {
      System.out.write(Jsons.serialize(iterator).getBytes(Charsets.UTF_8));
    }
  }

  private static Command parseCommand(String[] args) {
    final CommandLineParser parser = new RelaxedParser();
    final HelpFormatter helpFormatter = new HelpFormatter();

    final Options options = new Options();
    options.addOptionGroup(commandGroup);

    try {
      final CommandLine parsed = parser.parse(options, args);
      return Command.valueOf(parsed.getOptions()[0].getLongOpt().toUpperCase());
      // if discover, then validate, etc...
    } catch (ParseException e) {
      LOGGER.error(e.toString());
      helpFormatter.printHelp("singer-base", options);
      throw new IllegalArgumentException();
    }
  }

  private static Map<String, String> parseOptions(String[] args, Command command) {
    final CommandLineParser parser = new DefaultParser();
    final HelpFormatter helpFormatter = new HelpFormatter();

    final Options options = new Options();
    options.addOptionGroup(commandGroup); // so that the parser does not throw an exception when encounter command args.

    if (command.equals(Command.CHECK)) {
      options.addOption(Option.builder()
          .longOpt(Inputs.CONFIG.toString().toLowerCase())
          .desc("path to the json configuration file")
          .hasArg(true)
          .required(true)
          .build());
    }

    if (command.equals(Command.DISCOVER)) {
      options.addOption(Option.builder()
          .longOpt(Inputs.CONFIG.toString().toLowerCase())
          .desc("path to the json configuration file")
          .hasArg(true)
          .required(true)
          .build());
      options.addOption(Option.builder()
          .longOpt(Inputs.SCHEMA.toString().toLowerCase())
          .desc("output path for the discovered schema")
          .hasArg(true)
          .build());
    }

    if (command.equals(Command.READ)) {
      options.addOption(Option.builder()
          .longOpt(Inputs.CONFIG.toString().toLowerCase()).desc("path to the json configuration file")
          .hasArg(true)
          .required(true)
          .build());
      options.addOption(Option.builder()
          .longOpt(Inputs.SCHEMA.toString().toLowerCase()).desc("input path for the schema")
          .hasArg(true)
          .build());
      options.addOption(Option.builder()
          .longOpt(Inputs.STATE.toString().toLowerCase()).desc("path to the json-encoded state file")
          .hasArg(true)
          .build());
    }

    if (command.equals(Command.READ)) {
      options.addOption(Option.builder()
          .longOpt(Inputs.CONFIG.toString().toLowerCase())
          .desc("path to the json configuration file")
          .hasArg(true)
          .required(true)
          .build());
    }

    try {
      final CommandLine parse = parser.parse(options, args);
      return Arrays.stream(parse.getOptions()).collect(Collectors.toMap(Option::getLongOpt, Option::getValue));
    } catch (ParseException e) {
      LOGGER.error(e.toString());
      helpFormatter.printHelp(command.toString().toLowerCase(), options);
      throw new IllegalArgumentException();
    }
  }

  // https://stackoverflow.com/questions/33874902/apache-commons-cli-1-3-1-how-to-ignore-unknown-arguments
  private static class RelaxedParser extends DefaultParser {

    @Override
    public CommandLine parse(final Options options, final String[] arguments) throws ParseException {
      final List<String> knownArgs = new ArrayList<>();
      for (int i = 0; i < arguments.length; i++) {
        if (options.hasOption(arguments[i])) {
          knownArgs.add(arguments[i]);
          if (i + 1 < arguments.length && options.getOption(arguments[i]).hasArg()) {
            knownArgs.add(arguments[i + 1]);
          }
        }
      }
      return super.parse(options, knownArgs.toArray(new String[0]));
    }
  }

  public static void main(String[] args) throws IOException {
    new SingerBase().run(args);
  }
}
