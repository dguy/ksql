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

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import java.lang.reflect.Method;

public class UdfCompiler {

  public CustomUdf compile(final Method method, final UdfClassLoader loader) {
    try {
      final IScriptEvaluator scriptEvaluator
          = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
      scriptEvaluator.setClassName("TheCustomUdf");
      final String udfClassName = method.getDeclaringClass().getName();
      scriptEvaluator.setDefaultImports(new String[]{"java.util.*", udfClassName});
      scriptEvaluator.setParentClassLoader(loader);

      final StringBuilder builder = new StringBuilder()
          .append("return ((").append(method.getDeclaringClass().getSimpleName()).append(") thiz).")
          .append(method.getName()).append("(");

      int i = 0;
      for (Class<?> theClass : method.getParameterTypes()) {
        builder.append("(").append(theClass.getSimpleName()).append(")")
            .append("args[").append(i).append("]").append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(");");
      return (CustomUdf) scriptEvaluator.createFastEvaluator(builder.toString(),
          CustomUdf.class, new String[]{"thiz", "args"});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
