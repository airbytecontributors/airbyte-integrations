package io.airbyte.integrations.acceptance_tests.destination;

import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.io.PrintWriter;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

public class Utils {

  public static void runTestClass(Class<?> testClass) {
    final LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
        .selectors(selectClass(testClass))
        .build();

    final TestPlan plan = LauncherFactory.create().discover(request);
    final Launcher launcher = LauncherFactory.create();

    // Register a listener of your choice
    final SummaryGeneratingListener listener = new SummaryGeneratingListener();

    launcher.execute(plan, listener);


    // TODO display "pretty" error messages for each test that fails. Right now it's a bloodbath of stack traces. Seeing as this runner is mainly
    //  meant for non-Java devs to consume, it's important that failures are reported in an easily consumable way.
    listener.getSummary().printFailuresTo(new PrintWriter(System.out));
    listener.getSummary().printTo(new PrintWriter(System.out));

    if (listener.getSummary().getTestsFailedCount() > 0) {
      System.out.println(
          // TODO spruce up the docs page
          "There are failing tests. See https://docs.airbyte.io/connector-development/testing-connectors/ " +
              "for more information about the standard source test suite.");
      System.exit(1);
    }
  }
}
