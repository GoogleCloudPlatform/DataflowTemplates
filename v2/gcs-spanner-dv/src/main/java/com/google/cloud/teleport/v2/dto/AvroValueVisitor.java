package com.google.cloud.teleport.v2.dto;

import org.apache.avro.Schema;

/**
 * Visitor interface for Avro values.
 */
public interface AvroValueVisitor {
  void visitString(String s);

  void visitInt(int i);

  void visitLong(long l);

  void visitFloat(float f);

  void visitDouble(double d);

  void visitBoolean(boolean b);

  void visitBytes(byte[] b);

  void visitNull();

  void visitDefault(Object value);

  static void dispatch(Object value, Schema schema, AvroValueVisitor visitor) {
    if (value == null) {
      visitor.visitNull();
      return;
    }

    // Handle unions by finding the actual type if possible, or just dispatch based
    // on value type
    if (schema.getType() == Schema.Type.UNION) {
      // In a union, we might need to deduce the schema from the value if we can,
      // but commonly we just switch on the value's runtime type or use the schema
      // provided.
      // For simple visitation, checking the runtime type of 'value' is often robust
      // enough for primitives.
      // However, for complex types, we might need the specific schema branch.
      // For simplicity in this hashing context, we'll dispatch based on runtime type
      // where clear,
      // and use visitDefault for complex stuff.
    }

    switch (schema.getType()) {
      case STRING:
        visitor.visitString(value.toString()); // Avro strings can be Utf8
        break;
      case INT:
        visitor.visitInt((Integer) value);
        break;
      case LONG:
        visitor.visitLong((Long) value);
        break;
      case FLOAT:
        visitor.visitFloat((Float) value);
        break;
      case DOUBLE:
        visitor.visitDouble((Double) value);
        break;
      case BOOLEAN:
        visitor.visitBoolean((Boolean) value);
        break;
      case BYTES:
        if (value instanceof java.nio.ByteBuffer) {
          visitor.visitBytes(((java.nio.ByteBuffer) value).array());
        } else {
          visitor.visitBytes((byte[]) value);
        }
        break;
      case NULL:
        visitor.visitNull();
        break;
      case UNION:
        // Recurse using the value's likely type.
        // Note: GenericRecord.get() returns the value.
        // If it's a union, it returns the actual value, not a Union object.
        // We can try to infer the type.
        dispatchUnion(value, visitor);
        break;
      default:
        visitor.visitDefault(value);
    }
  }

  static void dispatchUnion(Object value, AvroValueVisitor visitor) {
    if (value instanceof CharSequence) {
      visitor.visitString(value.toString());
    } else if (value instanceof Integer) {
      visitor.visitInt((Integer) value);
    } else if (value instanceof Long) {
      visitor.visitLong((Long) value);
    } else if (value instanceof Float) {
      visitor.visitFloat((Float) value);
    } else if (value instanceof Double) {
      visitor.visitDouble((Double) value);
    } else if (value instanceof Boolean) {
      visitor.visitBoolean((Boolean) value);
    } else if (value instanceof java.nio.ByteBuffer) {
      visitor.visitBytes(((java.nio.ByteBuffer) value).array());
    } else if (value instanceof byte[]) {
      visitor.visitBytes((byte[]) value);
    } else {
      visitor.visitDefault(value);
    }
  }
}
