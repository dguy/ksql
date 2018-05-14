/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql;

import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfClass;

@UdfClass(name = "say_hello", description = "prefixes a string  with hello", returnType = String.class)
public class TestUdf {

  @Udf
  public String evaluate(final String input) {
    return "hello " + input;
  }

  @Udf
  public String evaluate(final Number input) {
    return "hello " + input;
  }

  public String map(final int bar, final Map<String, Integer> val, List<String> foo) {return "foo";}



  @Test
  public void should() throws NoSuchMethodException {
    Method map = TestUdf.class.getMethod("map", int.class, Map.class, List.class);
    Type[] genericParameterTypes = map.getGenericParameterTypes();
    System.out.println(genericParameterTypes);
  }
}
