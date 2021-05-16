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

package io.airbyte.protocol.models;

import java.util.Objects;

public class Field {

  /**
   * This types should match the cast_property_type function in stream_process.py.
   * See https://github.com/airbytehq/airbyte/blob/6ffada861bf1f3f04da885d0e6db61ec0339855e/airbyte-integrations/bases/base-normalization/normalization/transform_catalog/stream_processor.py#L361.
   */
  public enum JsonSchemaType {
    // Primitive Types
    STRING,
    NUMBER,
    OBJECT,
    ARRAY,
    BOOLEAN,
    // Custom Types
    JSON,
    NULL
  }

  private final String name;
  private final JsonSchemaType type;

  public Field(String name, JsonSchemaType type) {
    this.name = name;
    this.type = type;
  }

  public static Field of(String name, JsonSchemaType type) {
    return new Field(name, type);
  }

  public String getName() {
    return name;
  }

  public String getTypeAsJsonSchemaString() {
    return type.name().toLowerCase();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    return name.equals(field.name) &&
        type == field.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

}
