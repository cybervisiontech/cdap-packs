/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.lib.etl.schema;

import co.cask.cdap.api.common.Bytes;

/**
 * Type of the value of {@link Field} of data record.
 * NOTE: types casting should be optimized
 */
public enum FieldType {


  STRING,
  INT,
  LONG,
  DOUBLE,
  FLOAT;

  public <T> T fromBytes(byte[] value) {
    switch (this) {
      case STRING: return (T) Bytes.toString(value);
      case INT: return (T) Integer.valueOf(Bytes.toInt(value));
      case LONG: return (T) Long.valueOf(Bytes.toLong(value));
      case FLOAT: return (T) Float.valueOf(Bytes.toFloat(value));
      case DOUBLE: return (T) Double.valueOf(Bytes.toDouble(value));
      default: throw new IllegalArgumentException("Unknown type: " + this);
    }
  }

  public <T> byte[] toBytes(T value) {
    Object casted = cast(value);
    switch (this) {
      case STRING: return Bytes.toBytes((String) casted);
      case INT: return Bytes.toBytes((Integer) casted);
      case LONG: return Bytes.toBytes((Long) casted);
      case FLOAT: return Bytes.toBytes((Float) casted);
      case DOUBLE: return Bytes.toBytes((Double) casted);
      default: throw new IllegalArgumentException("Unknown type: " + this);
    }
  }

  public <IN, OUT> OUT cast(IN value) {
    if (value == null) {
      return null;
    }

    Class<?> cls = value.getClass();

    if (cls == Double.class) {
      return fromDouble((Double) value, cls);
    } else if (cls == Float.class) {
      return fromFloat((Float) value, cls);
    } else if (cls == Long.class) {
      return fromLong((Long) value, cls);
    } else if (cls == Integer.class) {
      return fromInteger((Integer) value, cls);
    } else if (cls == String.class) {
      return fromString((String) value, cls);
    } else if (cls == Short.class) {
      return fromLong(((Short) value).longValue(), cls);
    } else if (cls == Byte.class) {
      return fromLong(((Byte) value).longValue(), cls);
    } else {
      throw new IllegalArgumentException(cls.getName() + " cannot be casted to type " + this);
    }
  }

  private <OUT> OUT fromInteger(Integer val, Class<?> cls) {
    switch (this) {
      case DOUBLE:
        return (OUT) new Double(val.doubleValue());
      case FLOAT:
        return (OUT) new Float(val.floatValue());
      case LONG:
        return (OUT) new Long(val.longValue());
      case INT:
        return (OUT) val;
      case STRING:
        return (OUT) val.toString();
      default:
        throw new IllegalArgumentException(String.format("Cannot convert %s to %s", cls, this));
    }
  }


  private <OUT> OUT fromLong(Long val, Class<?> cls) {
    switch (this) {
      case DOUBLE:
        return (OUT) new Double(val.doubleValue());
      case FLOAT:
        return (OUT) new Float(val.floatValue());
      case LONG:
        return (OUT) val;
      case INT:
        return (OUT) new Integer(val.intValue());
      case STRING:
        return (OUT) val.toString();
      default:
        throw new IllegalArgumentException(String.format("Cannot convert %s to %s", cls, this));
    }
  }

  private <OUT> OUT fromFloat(Float val, Class<?> cls) {
    switch (this) {
      case DOUBLE:
        return (OUT) new Double(val.doubleValue());
      case FLOAT:
        return (OUT) val;
      case LONG:
        return (OUT) new Long(val.longValue());
      case INT:
        return (OUT) new Integer(val.intValue());
      case STRING:
        return (OUT) val.toString();
      default:
        throw new IllegalArgumentException(String.format("Cannot convert %s to %s", cls, this));
    }
  }

  private <OUT> OUT fromDouble(Double val, Class<?> cls) {
    switch (this) {
      case DOUBLE:
        return (OUT) val;
      case FLOAT:
        return (OUT) new Float(val.floatValue());
      case LONG:
        return (OUT) new Long(val.longValue());
      case INT:
        return (OUT) new Integer(val.intValue());
      case STRING:
        return (OUT) val.toString();
      default:
        throw new IllegalArgumentException(String.format("Cannot convert %s to %s", cls, this));
    }
  }

  private <OUT> OUT fromString(String val, Class<?> cls) {
    switch (this) {
      case DOUBLE:
        return (OUT) Double.valueOf(val);
      case FLOAT:
        return (OUT) Float.valueOf(val);
      case LONG:
        return (OUT) Long.valueOf(val);
      case INT:
        return (OUT) Integer.valueOf(val);
      case STRING:
        return (OUT) val;
      default:
        throw new IllegalArgumentException(String.format("Cannot convert %s to %s", cls, this));
    }
  }

  public Object getDefaultValue() {
    switch (this) {
      case DOUBLE:
        return Double.valueOf(0);
      case FLOAT:
        return Float.valueOf(0);
      case LONG:
        return Long.valueOf(0);
      case INT:
        return Integer.valueOf(0);
      case STRING:
        return "";
      default:
        throw new IllegalArgumentException(String.format("Invalid type: %s", this));
    }
  }
}
