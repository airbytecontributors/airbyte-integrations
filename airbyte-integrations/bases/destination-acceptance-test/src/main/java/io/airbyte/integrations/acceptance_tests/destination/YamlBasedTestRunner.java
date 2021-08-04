package io.airbyte.integrations.acceptance_tests.destination;

import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.yaml.Yamls;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.nio.file.Paths;

import static io.airbyte.integrations.acceptance_tests.destination.Utils.runTestClass;

public class YamlBasedTestRunner {

  /**
   * @param args expects a --test-config flag points to a yaml file which contains the configuration described by
   *             {@link ExternallyConfigurableDestinationAcceptanceTest.TestConfiguration}
   */
  public static void main(String[] args) throws ArgumentParserException {
    ArgumentParser parser = ArgumentParsers.newFor(YamlBasedTestRunner.class.getName()).build()
        .defaultHelp(true)
        .description("Run standard source tests");

    parser.addArgument("--test-config")
        .required(true)
        .help("Path to a YAML file");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    String configPath = ns.getString("test_config");
    System.out.println(configPath);
    String yamlConfig = IOs.readFile(Paths.get(configPath));
    InputConfiguration deserialize = Yamls.deserialize(yamlConfig, InputConfiguration.class);
    System.out.println(Jsons.serialize(deserialize));
    // read yaml configuration

    //    runTestClass(ExternallyConfigurableDestinationAcceptanceTest.class);
  }

}
