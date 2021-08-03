package io.airbyte.integrations.acceptance_tests.destination;

import io.airbyte.commons.io.IOs;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;

import java.nio.file.Paths;

import static io.airbyte.integrations.acceptance_tests.destination.Utils.runTestClass;

public class YamlBasedTestRunner {

  /**
   *
   * @param args expects a --test-config flag points to a yaml file which contains the configuration described by
   *             {@link ExternallyConfigurableDestinationAcceptanceTest.TestConfiguration}
   */
  public static void main(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor(YamlBasedTestRunner.class.getName()).build()
        .defaultHelp(true)
        .description("Run standard source tests");

    parser.addArgument("--test-config")
        .required(true)
        .help("Path to a YAML file ");


    IOs.readFile(Paths.get())
    // read yaml configuration

    runTestClass(ExternallyConfigurableDestinationAcceptanceTest.class);
  }
  

}
