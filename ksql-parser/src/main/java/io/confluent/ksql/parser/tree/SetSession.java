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

package io.confluent.ksql.parser.tree;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SetSession
    extends Statement {

  private final QualifiedName name;
  private final Expression value;

  public SetSession(QualifiedName name, Expression value) {
    this.name = name;
    this.value = value;
  }

  public QualifiedName getName() {
    return name;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSetSession(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    SetSession o = (SetSession) obj;
    return Objects.equals(name, o.name)
           && Objects.equals(value, o.value);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("value", value)
        .toString();
  }
}
