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

package io.confluent.ksql.function;

import org.apache.kafka.common.metrics.Metrics;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.resource.Resource;
import org.codehaus.janino.util.resource.ResourceFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdfArgSupplier;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

/**
 * This class takes methods that have been marked with the Udf or UdfFactory annotation.
 * Each method gets a class generate for it. For Udfs it is an {@link UdfInvoker}.
 * For UDAFs it is a {@link KsqlAggregateFunction}
 */
public class UdfCompiler {
  private static final Logger LOGGER = LoggerFactory.getLogger(UdfCompiler.class);

  private static final Map<Class, Function<Integer, String>> typeConverters
      = ImmutableMap.<Class, Function<Integer, String>>builder()
      .put(int.class, index -> typeConversionCode("Integer", index))
      .put(Integer.class, index -> typeConversionCode("Integer", index))
      .put(long.class, index -> typeConversionCode("Long", index))
      .put(Long.class, index -> typeConversionCode("Long", index))
      .put(double.class, index -> typeConversionCode("Double", index))
      .put(Double.class, index -> typeConversionCode("Double", index))
      .put(boolean.class, index -> typeConversionCode("Boolean", index))
      .put(Boolean.class, index -> typeConversionCode("Boolean", index))
      .put(String.class, index -> typeConversionCode("String", index))
      .put(Map.class, index -> typeConversionCode("Map", index))
      .put(List.class, index -> typeConversionCode("List", index))
      .build();

  private static final Map<Class, Function<Integer, String>> aggregateParamTypes =
      ImmutableMap.<Class, Function<Integer, String>>builder()
          .put(int.class,
              index -> "Integer.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(Integer.class,
              index -> "Integer.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(long.class,
              index -> "Long.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(Long.class,
              index -> "Long.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(double.class,
              index -> "Double.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(Double.class,
              index -> "Double.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(boolean.class,
              index -> "Boolean.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(Boolean.class,
              index -> "Boolean.valueOf(aggregateFunctionArguments.arg(" + (index + 1) + "))")
          .put(String.class, index -> "aggregateFunctionArguments.arg(" + (index + 1) + ")")
          .build();

  // Templates used to generate the UDF code
  private static final String genericTemplate =
      "#TYPE arg#INDEX;\n"
          + "if(args[#INDEX] == null) arg#INDEX = null;\n"
          + "else if (args[#INDEX] instanceof #TYPE) arg#INDEX = (#TYPE)args[#INDEX];\n"
          + "else if (args[#INDEX] instanceof String) \n"
          + "   try {\n"
          + "       arg#INDEX = #TYPE.valueOf((String)args[#INDEX]);\n"
          + "   } catch (Exception e) {\n"
          + "     throw new KsqlFunctionException(\"Couldn't coerce string argument'\" "
          + "+ args[#INDEX] + \"' to expected type  #TYPE\");\n"
          + "   }\n";


  private static final String INTEGER_NUMBER_TEMPLATE =
      "else if (args[#INDEX] instanceof Number) arg#INDEX = "
          + "((Number)args[#INDEX]).intValue();\n";

  private static final String NUMBER_TEMPLATE =
      "else if (args[#INDEX] instanceof Number) arg#INDEX = "
          + "((Number)args[#INDEX]).#LC_TYPEValue();\n";

  private static final String THROWS_TEMPLATE =
      "else throw new KsqlFunctionException(\"Type: \" + args[#INDEX].getClass() + \""
          + " is not supported by KSQL UDFS\");";

  private final String udafTemplate;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Metrics> metrics;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public UdfCompiler(final Optional<Metrics> metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics can't be null");
    try (final InputStream inputStream = getClass().getClassLoader()
        .getResourceAsStream("KsqlAggregateFunctionTemplate.java")) {
      final Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name());
      final StringBuilder builder = new StringBuilder();
      while (scanner.hasNextLine()) {
        builder.append(scanner.nextLine()).append("\n");
      }
      udafTemplate = builder.toString();
    } catch (final IOException io) {
      throw new KsqlException("Couldn't load UDAF template", io);
    }
  }

  UdfInvoker compile(final Method method, final ClassLoader loader) {
    try {
      final IScriptEvaluator scriptEvaluator = createScriptEvaluator(method,
          loader,
          method.getDeclaringClass().getName());
      final String code = generateCode(method);
      return (UdfInvoker) scriptEvaluator.createFastEvaluator(code,
          UdfInvoker.class, new String[]{"thiz", "args"});
    } catch (final KsqlException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile udf wrapper class for "
          + method, e);
    }
  }

  @SuppressWarnings("unchecked")
  KsqlAggregateFunction compileAggregate(final Method method,
                                         final ClassLoader loader,
                                         final String functionName,
                                         final Class aggregateClass,
                                         final Class valueClass) {
    validateReturnType(method, aggregateClass, valueClass, functionName);
    try {
      final String generatedClassName
          = method.getDeclaringClass().getSimpleName() + "_" + method.getName() + "_Aggregate";
      final String udafClass = generateUdafClass(generatedClassName, method, functionName);
      LOGGER.debug("Generated class for functionName={}, method={} class{}\n",
          functionName,
          method.getName(),
          udafClass);
      final ClassLoader javaSourceClassLoader
          = createJavaSourceClassLoader(loader, generatedClassName, udafClass);

      final String fqn = "io.confluent.ksql.function.udaf." + generatedClassName;
      final IScriptEvaluator scriptEvaluator =
          createScriptEvaluator(method,
              javaSourceClassLoader,
              fqn);
      final UdfArgSupplier evaluator = (UdfArgSupplier)
          scriptEvaluator.createFastEvaluator("return new " + generatedClassName
                  + "(args, returnType, metrics);",
              UdfArgSupplier.class, new String[]{"args", "returnType", "metrics"});
      return evaluator.apply(Collections.singletonList(SchemaUtil.getSchemaFromType(valueClass)),
          SchemaUtil.getSchemaFromType(aggregateClass), metrics);
    } catch (final Exception e) {
      throw new KsqlException("Failed to compile KSqlAggregateFunction for method='"
          + method.getName() + "' in class='" + method.getDeclaringClass() + "'", e);
    }
  }

  private void validateReturnType(final Method method,
                                  final Class aggregateClass,
                                  final Class valueClass,
                                  final String functionName) {
    final String functionInfo = "method='" + method.getName()
        + "', functionName='" + functionName + "' UDFClass='" + method.getDeclaringClass() + "'";
    final String invalidClass = "class='%s'"
        + " is not supported by UDAFs. " + functionInfo;
    if (!typeConverters.containsKey(aggregateClass)) {
      throw new KsqlException(String.format(invalidClass, aggregateClass));
    }
    if (!typeConverters.containsKey(valueClass)) {
      throw new KsqlException(String.format(invalidClass, valueClass));
    }
    if (!Udaf.class.isAssignableFrom(method.getReturnType())) {
      throw new KsqlException("UDAFs must implement " + Udaf.class.getName() + " or "
          + TableUdaf.class.getName() + " ."
          + functionInfo);
    }
  }


  private JavaSourceClassLoader createJavaSourceClassLoader(final ClassLoader loader,
                                                            final String generatedClassName,
                                                            final String udafClass) {
    final long lastMod = System.currentTimeMillis();
    return new JavaSourceClassLoader(loader, new ResourceFinder() {
      @Override
      public Resource findResource(final String resource) {
        if (resource.endsWith(generatedClassName + ".java")) {
          return new Resource() {
            @Override
            public InputStream open() throws IOException {
              return new ByteArrayInputStream(udafClass.getBytes(StandardCharsets.UTF_8.name()));
            }

            @Override
            public String getFileName() {
              return resource;
            }

            @Override
            public long lastModified() {
              return lastMod;
            }
          };
        }
        return null;
      }
    }, StandardCharsets.UTF_8.name());
  }

  private String generateUdafClass(final String generatedClassName,
                                   final Method method,
                                   final String functionName) {
    final Class<?>[] params = method.getParameterTypes();
    final String udafFactoryArguments = IntStream.range(0, params.length)
        .mapToObj(index -> {
          final Function<Integer, String> typeConversion = aggregateParamTypes.get(params[index]);
          if (typeConversion == null) {
            throw new KsqlException(String.format("Type %s is not supported by UDAF Factory "
                    + "methods. Supported types %s. functionName=%s, method=%s, class=%s",
                params[index],
                aggregateParamTypes,
                functionName,
                method.getName(),
                method.getDeclaringClass()));
          }
          return typeConversion.apply(index);
        }).collect(Collectors.joining(","));

    final boolean supportsTableAgg = TableUdaf.class.isAssignableFrom(method.getReturnType());
    return udafTemplate.replaceAll("#FUNCTION_CLASS_NAME", generatedClassName)
        .replaceAll("#CLASS", method.getDeclaringClass().getName())
        .replaceAll("#METHOD", method.getName())
        .replaceAll("#RETURN_TYPE", "SchemaBuilder.string()")
        .replaceAll("#NAME", functionName)
        .replaceAll("#ARGS", udafFactoryArguments)
        .replaceAll("#ARG_COUNT", String.valueOf(params.length + 1))
        .replaceAll("#ADD_TABLE_AGG", supportsTableAgg
            ? "implements TableAggregationFunction"
            : "");
  }


  /**
   * Generates code for the given method. Assuming the method has a single Boolean argument
   * the generated code would look like the below. A block of code like this is generated for each
   * of the arguments.
   *<pre>{@code
   * // try and coerce the arguments to the types expected by the UDF
   * Boolean arg0;
   * if(args[0] == null) arg0 = null;
   * else if(args[0] instanceof Boolean) arg0 = (Boolean)args[0];
   * else if(args[0] instanceof String) arg0 = Boolean.valueOf((String)args[0]);
   * else throw new KsqlFunctionException("Type: " + args[0].getClass()
   * + "is not supported by KSQL UDFS");
   *
   * // invoke the udf with the args
   * return ((UdfCompilerTest) thiz).udf(arg0);
   *}</pre>
   * @param method  the UDF to generate the code for
   * @return String representation of the code that should be compiled for the UDF
   */

  private static String generateCode(final Method method) {
    final Class<?>[] params = method.getParameterTypes();

    final String prefix = IntStream.range(0, params.length).mapToObj(i -> {
      final Function<Integer, String> converter = typeConverters.get(params[i]);
      if (converter == null) {
        throw new KsqlException("Type " + params[i] + " is not supported in UDFs");
      }
      return converter.apply(i);
    }).collect(Collectors.joining("\n", "", "\nreturn (("
        + method.getDeclaringClass().getSimpleName()
        + ") thiz)." + method.getName() + "("
    ));

    final String code = IntStream.range(0, params.length).mapToObj(i -> "arg" + i)
        .collect(Collectors.joining(",",
            prefix, ");"));

    LOGGER.debug("generated code for udf method = {}\n{}", method, code);
    return code;
  }

  private static IScriptEvaluator createScriptEvaluator(final Method method,
                                                        final ClassLoader loader,
                                                        final String udfClass) throws Exception {
    final IScriptEvaluator scriptEvaluator
        = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    scriptEvaluator.setClassName(method.getDeclaringClass().getName() + "_" + method.getName());
    scriptEvaluator.setDefaultImports(new String[]{
        "java.util.*",
        "io.confluent.ksql.function.KsqlFunctionException",
        udfClass,
    });
    scriptEvaluator.setParentClassLoader(loader);
    return scriptEvaluator;
  }


  private static String typeConversionCode(final String type, final int index) {
    if (type.equals("Map") || type.equals("List")) {
      return type + " arg" + index + " = (" + type + ")args[" + index + "];\n";
    }

    final StringBuilder builder = new StringBuilder();
    builder.append(genericTemplate);
    if (type.equals("Integer")) {
      builder.append(INTEGER_NUMBER_TEMPLATE);
    } else if (!type.equals("String") && !type.equals("Boolean")) {
      builder.append(NUMBER_TEMPLATE);
    }
    builder.append(THROWS_TEMPLATE);
    return builder.toString()
        .replaceAll("#TYPE", type)
        .replaceAll("#LC_TYPE", type.toLowerCase())
        .replaceAll("#INDEX", String.valueOf(index));
  }

}
