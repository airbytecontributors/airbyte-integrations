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

package io.airbyte.integrations.scratch.exchangerate;

import com.google.common.collect.Lists;
import io.airbyte.integrations.scratch.AirbyteCheckResponse;
import io.airbyte.integrations.scratch.AirbyteMessage;
import io.airbyte.integrations.scratch.AirbyteSchema;
import io.airbyte.integrations.scratch.AirbyteSpec;
import io.airbyte.integrations.scratch.Integration;
import java.util.HashMap;
import java.util.Iterator;
import org.slf4j.Logger;

public class ExchangeRateSource implements Integration<ExchangeRateConfig> {

  @Override
  public AirbyteSpec spec() {
    // todo
    return new ExchangeRateSpec();
  }

  @Override
  public ExchangeRateConfig readConfig(String strConfig) throws Exception {
    // todo: just hardcode this now, not important for interface
    return new ExchangeRateConfig("USD", "2020-09-22");
  }

  @Override
  public AirbyteCheckResponse check(Logger logger, ExchangeRateConfig exchangeRateConfig) {
    // todo: could check if there's actually a connection
    return new AirbyteCheckResponse(true, new HashMap<>());
  }

  @Override
  public AirbyteSchema discover(Logger logger, ExchangeRateConfig exchangeRateConfig) {
    // todo: should also support failure modes
    return new AirbyteSchema();
  }

  @Override
  public Iterator<AirbyteMessage> read(Logger logger, ExchangeRateConfig exchangeRateConfig) {
    // read here is not a per-record operation, it's per iterator, which isn't possible on the command
    // line
    // todo: actually read form API
    return Lists.newArrayList(new AirbyteMessage()).iterator();
  }

}
