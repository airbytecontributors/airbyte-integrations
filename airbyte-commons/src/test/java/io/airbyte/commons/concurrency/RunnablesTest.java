/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.commons.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

public class RunnablesTest {

  @Test
  void testInheritsParentMdc2() {
    MDC.put("foo", "bar");

    final Runnable inheritedMdcRunnable = Runnables.wrapWithParentMdc(() -> {
      // if the MDC is not inherited we would expect this assertion to fail with the MDC being empty.
      assertEquals(ImmutableMap.of("foo", "bar"), MDC.getCopyOfContextMap());
    });

    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    inheritedMdcRunnable.run();

    assertEquals(ImmutableMap.of("foo", "bar"), MDC.getCopyOfContextMap());
  }


  @Test
  void testInheritsParentMdc() throws ExecutionException, InterruptedException, TimeoutException {
    MDC.setContextMap(ImmutableMap.of("foo", "bar"));

    final Runnable inheritedMdcRunnable = Runnables.wrapWithParentMdc(() -> {
      // if the MDC is not inherited we would expect this assertion to fail with the MDC being empty.
      assertEquals(ImmutableMap.of("foo", "bar"), MDC.getCopyOfContextMap());
    });

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future<?> inheritedMdcRunnableFuture = executorService.submit(inheritedMdcRunnable);
    inheritedMdcRunnableFuture.get(1, TimeUnit.MINUTES);

    executorService.shutdown();
  }

  @Test
  void testClearsMdc() throws InterruptedException, ExecutionException, TimeoutException {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    MDC.setContextMap(ImmutableMap.of("foo", "bar"));

    final Runnable inheritedMdcRunnable = Runnables.wrapWithParentMdc(() -> {
      MDC.put("foo1", "bar1");
      assertEquals(ImmutableMap.of("foo", "bar", "foo1", "bar1"), MDC.getCopyOfContextMap());
    });

    final Future<?> inheritedMdcRunnableFuture = executorService.submit(inheritedMdcRunnable);
    inheritedMdcRunnableFuture.get(1, TimeUnit.MINUTES);

    final Runnable assertionRunnable = () -> {
      // if the abstraction abstraction is not properly cleaning up the MDC, we would see entries in the
      // MDC from previous runnables. e.g. { "foo": "bar", "foo0": "bar0", "foo1": "bar1" }
      assertEquals(Collections.emptyMap(), MDC.getCopyOfContextMap());
    };
    final Future<?> assertionRunnableFuture = executorService.submit(assertionRunnable);
    assertionRunnableFuture.get(1, TimeUnit.MINUTES);

    executorService.shutdown();
  }

  @Test
  void testClearsMdcOnFailure() throws InterruptedException, ExecutionException, TimeoutException {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    MDC.setContextMap(ImmutableMap.of("foo", "bar"));

    final Runnable exceptionThrowingRunnable = Runnables.wrapWithParentMdc(() -> {
      MDC.put("foo1", "bar1");

      throw new RuntimeException("induced exception");
    });

    final Runnable assertionRunnable = () -> {
      // this is a "normal" runnable. we do not expect it to inherit MDC from the parent. we want to make
      // sure it doesn't inherit MDC from the previous runnable either. If the previous runnable does not
      // clean up properly than we will.
      assertEquals(Collections.emptyMap(), MDC.getCopyOfContextMap());
    };

    final Future<?> exceptionThrowingRunnableFuture = executorService.submit(exceptionThrowingRunnable);
    assertThrows(ExecutionException.class, () -> exceptionThrowingRunnableFuture.get(1, TimeUnit.MINUTES));

    final Future<?> assertionRunnableFuture = executorService.submit(assertionRunnable);
    assertionRunnableFuture.get(1, TimeUnit.MINUTES);

    executorService.shutdown();
  }

}
