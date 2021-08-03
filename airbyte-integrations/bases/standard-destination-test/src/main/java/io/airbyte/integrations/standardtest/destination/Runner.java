package io.airbyte.integrations.acceptance_tests.destination;

import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.io.PrintWriter;
import java.nio.file.Path;

import static io.airbyte.integrations.acceptance_tests.destination.Utils.runTestClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

public class Runner {

  public static void main(String[] args) {

    runTestClass(ExternallyConfigurableDestinationAcceptanceTest.class);
  }
  

}
