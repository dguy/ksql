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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.ksql.function.udf.PluginUdf;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfClass;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

public class UdfLoader {

  private static final Logger logger = LoggerFactory.getLogger(UdfLoader.class);

  private final MetaStore metaStore;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final Blacklister blacklist;
  private final UdfCompiler compiler;


  public UdfLoader(final MetaStore metaStore,
                   final File pluginDir,
                   final ClassLoader parentClassLoader,
                   final Blacklister blacklist,
                   final UdfCompiler compiler) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore can't be null");
    this.pluginDir = Objects.requireNonNull(pluginDir, "pluginDir can't be null");
    this.parentClassLoader = Objects.requireNonNull(parentClassLoader,
        "parentClassLoader can't be null");
    this.blacklist = Objects.requireNonNull(blacklist, "blacklist can't be null");
    this.compiler = Objects.requireNonNull(compiler, "compiler can't be null");
  }

  public void load() {
    try {
      Files.find(pluginDir.toPath(), 1, (path, attributes) -> path.toString().endsWith(".jar"))
          .map(path -> UdfClassLoader.newClassLoader(path, parentClassLoader, blacklist))
          .forEach(this::loadUdfs);
    } catch (IOException e) {
      logger.error("Failed to load UDFs from location {}", pluginDir, e);
    }
  }

  private void loadUdfs(final UdfClassLoader loader) {
    new FastClasspathScanner()
        .overrideClassLoaders(loader)
        .ignoreParentClassLoaders()
        .matchClassesWithMethodAnnotation(Udf.class,
            (theClass, executable) -> {
              final UdfClass annotation = theClass.getAnnotation(UdfClass.class);
              if (annotation != null) {
                final Method method = (Method) executable;
                final CustomUdf udf = compiler.compile(method, loader);
                metaStore.addFunction(new KsqlFunction(
                    SchemaUtil.getSchemaFromClass(method.getReturnType()),
                    Arrays.stream(method.getGenericParameterTypes())
                        .map(SchemaUtil::getSchemaFromJavaType).collect(Collectors.toList()),
                    annotation.name(),
                    PluginUdf.class,
                    () -> {
                      try {
                        return new PluginUdf(udf, method.getDeclaringClass().newInstance());
                      } catch (Exception e) {
                        throw new KsqlException("Failed to create new udf instance for "
                            + method.getDeclaringClass().getName()
                            + "." + method.getName(),
                            e);
                      }
                    }));
              }
            }).scan();
  }

}
