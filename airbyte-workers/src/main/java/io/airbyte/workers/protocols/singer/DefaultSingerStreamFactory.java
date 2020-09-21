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

package io.airbyte.workers.protocols.singer;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.singer.SingerCatalog;
import io.airbyte.singer.SingerConfigSchema;
import io.airbyte.singer.SingerMessage;
import java.io.BufferedReader;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a stream from an input stream. The produced stream attempts to parse each line of the
 * InputStream into a SingerMessage. If the line cannot be parsed into a SingerMessage it is
 * dropped. Each record MUST be new line separated.
 *
 * <p>
 * If a line starts with a SingerMessage and then has other characters after it, that SingerMessage
 * will still be parsed. If there are multiple SingerMessage records on the same line, only the
 * first will be parsed.
 */
public class DefaultSingerStreamFactory<T> implements SingerStreamFactory<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSingerStreamFactory.class);

  private final SingerProtocolPredicate singerProtocolValidator;
  private final Logger logger;
  private final Class<T> clazz;

  public static DefaultSingerStreamFactory<SingerMessage> message() {
    return new DefaultSingerStreamFactory<>(new SingerProtocolPredicate(SingerConfigSchema.SINGER_MESSAGE), LOGGER, SingerMessage.class);
  }

  public static DefaultSingerStreamFactory<SingerCatalog> catalog() {
    return new DefaultSingerStreamFactory<>(new SingerProtocolPredicate(SingerConfigSchema.SINGER_CATALOG), LOGGER, SingerCatalog.class);
  }

  DefaultSingerStreamFactory(final SingerProtocolPredicate singerProtocolPredicate, final Logger logger, Class<T> clazz) {
    singerProtocolValidator = singerProtocolPredicate;
    this.logger = logger;
    this.clazz = clazz;
  }

  public Stream<T> create(BufferedReader bufferedReader) {
    return bufferedReader
        .lines()
        .map(s -> {
          Optional<JsonNode> j = Jsons.tryDeserialize(s);
          if (j.isEmpty()) {
            // we log as info all the lines that are not valid json
            // some taps actually logs their process on stdout, we
            // want to make sure this info is available in the logs.
            logger.info(s);
          }
          return j;
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(j -> {
          boolean res = singerProtocolValidator.test(j);
          if (!res) {
            logger.error("Validation failed: {}", Jsons.serialize(j));
          }
          return res;
        })
        .map(j -> {
          Optional<T> m = Jsons.tryObject(j, clazz);
          if (m.isEmpty()) {
            logger.error("Deserialization failed: {}", Jsons.serialize(j));
          }
          return m;
        })
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

}
