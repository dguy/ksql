/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.function.udaf;

@UdafDescription(name = "test_udaf", description = "test_udaf")
public class TestUdaf {

  @UdafFactory(description = "sums longs", aggregateType = Long.class, valueType = Long.class)
  public static Udaf<Long, Long> createSumLong() {
    return new Udaf<Long, Long>() {
      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long aggregate(final Long aggregate, final Long aLong) {
        return aggregate + aLong;
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }
    };
  }

  @UdafFactory(description = "sums double", aggregateType = Double.class, valueType = Double.class)
  public static Udaf<Double, Double> createSumDouble() {
    return new Udaf<Double, Double>() {
      @Override
      public Double initialize() {
        return 0.0;
      }

      @Override
      public Double aggregate(final Double aggregate, final Double val) {
        return aggregate + val;
      }

      @Override
      public Double merge(final Double aggOne, final Double aggTwo) {
        return aggOne + aggTwo;
      }
    };
  }

  @UdafFactory(description = "sums the length of strings",
      aggregateType = Long.class,
      valueType = String.class)
  public static Udaf<String, Long> createSumLengthString(final String initialString) {
    return new Udaf<String, Long>() {
      @Override
      public Long initialize() {
        return (long) initialString.length();
      }

      @Override
      public Long aggregate(final String s, final Long aggregate) {
        return aggregate + s == null ? 0L : s.length();
      }

      @Override
      public Long merge(final Long aggOne, final Long aggTwo) {
        return aggOne + aggTwo;
      }
    };
  }

}