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

import java.util.Map;
import org.slf4j.MDC;

// implementation taken from:
// https://www.chintanradia.com/blog/pass-mdc-context-from-calling-thread-to-new-thread/

/**
 * The goal of this class is to pass MDC of the thread that instantiates the Runnable to the execution of the Runnable. It also makes sure that the MDC of the Runnable is cleaned up after it executes (regardless of success or failure).
 * implementation taken from: https://www.chintanradia.com/blog/pass-mdc-context-from-calling-thread-to-new-thread/
 */
public class MdcInheritingRunnable implements Runnable {

  private final Runnable runnable;
  private final Map<String, String> context;

  public MdcInheritingRunnable(Runnable runnable) {
    this.runnable = runnable;
    this.context = MDC.getCopyOfContextMap();
  }

  @Override
  public void run() {
    try {
      MDC.setContextMap(context);
      runnable.run();
    } finally {
      MDC.clear();
    }
  }

}
