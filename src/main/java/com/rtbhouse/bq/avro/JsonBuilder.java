package com.rtbhouse.bq.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

public class JsonBuilder {

    public String build(GenericRecord avro) {
        StringBuilder buffer = new StringBuilder();
        toString(avro, buffer);
        return buffer.toString();
    }

    private void toString(Object datum, StringBuilder buffer) {
        if (isRecord(datum)) {
            buffer.append("{");
            int count = 0;
            Schema schema = getRecordSchema(datum);
            for (Schema.Field f : schema.getFields()) {
                Object field = getField(datum, f.pos());
                if (field == null) {
                    continue;
                } else if (count++ > 0) {
                    buffer.append(", ");
                }
                toString(f.name(), buffer);
                buffer.append(": ");
                toString(field, buffer);
            }
            buffer.append("}");
        } else if (isArray(datum)) {
            Collection<?> array = (Collection<?>) datum;
            buffer.append("[");
            long last = array.size() - 1;
            int i = 0;
            for (Object element : array) {
                toString(element, buffer);
                if (i++ < last) {
                    buffer.append(", ");
                }
            }
            buffer.append("]");
        } else if (isMap(datum)) {
            buffer.append("[");
            int count = 0;
            @SuppressWarnings(value = "unchecked")
            Map<Object, Object> map = (Map<Object, Object>) datum;
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                buffer.append("{\"key\": ");
                toString(entry.getKey(), buffer);
                buffer.append(", \"value\": ");
                toString(entry.getValue(), buffer);
                buffer.append("}");
                if (++count < map.size()) {
                    buffer.append(", ");
                }
            }
            buffer.append("]");
        } else if (isString(datum) || isEnum(datum)) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (isDouble(datum)) {
            buffer.append(Double.isNaN((Double) datum) ? "null" : datum);
        } else if (isFloat(datum)) {
            buffer.append(Float.isNaN((Float) datum) ? "null" : datum);
        } else if (isBytes(datum)) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = (ByteBuffer) datum;
            for (int i = bytes.position(); i < bytes.limit(); i++) {
                buffer.append((char) bytes.get(i));
            }
            buffer.append("\"}");
        } else {
            buffer.append(datum);
        }
    }

    private Object getField(Object record, int position) {
        return ((IndexedRecord) record).get(position);
    }

    private boolean isArray(Object datum) {
        return datum instanceof Collection;
    }

    private boolean isRecord(Object datum) {
        return datum instanceof IndexedRecord;
    }

    private Schema getRecordSchema(Object record) {
        return ((GenericContainer) record).getSchema();
    }

    private boolean isEnum(Object datum) {
        return datum instanceof GenericEnumSymbol || datum.getClass().isEnum();
    }

    private boolean isMap(Object datum) {
        return datum instanceof Map;
    }

    private boolean isString(Object datum) {
        return datum instanceof CharSequence;
    }

    private boolean isDouble(Object datum) {
        return datum instanceof Double;
    }

    private boolean isFloat(Object datum) {
        return datum instanceof Float;
    }

    private boolean isBytes(Object datum) {
        return datum instanceof ByteBuffer;
    }

    private boolean isBoolean(Object datum) {
        return datum instanceof Boolean;
    }

    /* Adapted from http://code.google.com/p/json-simple */
    private void writeEscapedString(String string, StringBuilder builder) {
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            switch (ch) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                case '/':
                    builder.append("\\/");
                    break;
                default:
                    // Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F') || (ch >= '\u2000' && ch <= '\u20FF')) {
                        String hex = Integer.toHexString(ch);
                        builder.append("\\u");
                        for (int j = 0; j < 4 - hex.length(); j++) {
                            builder.append('0');
                        }
                        builder.append(hex.toUpperCase());
                    } else {
                        builder.append(ch);
                    }
            }
        }
    }

}
