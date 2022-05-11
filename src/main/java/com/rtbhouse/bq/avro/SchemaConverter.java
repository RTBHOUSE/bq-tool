package com.rtbhouse.bq.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class SchemaConverter {

    private static final String TREPEATED = "REPEATED";

    private static final String TRECORD = "RECORD";

    private static final String TSTRING = "STRING";

    private static final String TFLOAT = "FLOAT";

    private static final String TINTEGER = "INTEGER";

    private static final String TBYTES = "BYTES";

    private static final String TNULLABLE = "NULLABLE";

    private static final String TBOOLEAN = "BOOLEAN";

    private static final List<Schema.Type> COMPLEX_TYPES
        = Lists.newArrayList(Schema.Type.RECORD, Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION);

    public String convert(Schema avroSchema) {
        if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
            throw new IllegalArgumentException("Avro schema must be a record.");
        }
        return "[" + convertFields(avroSchema.getFields()) + "]";
    }

    private String convertFields(List<Schema.Field> fields) {
        List<String> types = new ArrayList<>();
        fields.stream().filter((field) -> !(field.schema().getType().equals(Schema.Type.NULL)))
            .forEach((field) -> {
                types.add(convertField(field));
            });
        return StringUtils.join(types, ",");
    }

    private String convertField(String fieldName, Schema schema) {
        Schema.Type type = schema.getType();
        if (!COMPLEX_TYPES.contains(type)) {
            return field(fieldName, typeFor(schema));
        } else if (type.equals(Schema.Type.RECORD)) {
            return complex(fieldName, TRECORD, TNULLABLE, convertFields(schema.getFields()));
        } else if (type.equals(Schema.Type.ARRAY)) {
            if (Schema.Type.RECORD.equals(schema.getElementType().getType())) {
                return complex(fieldName, TRECORD, TREPEATED, convertFields(schema.getElementType().getFields()));
            } else {
                return field(fieldName, typeFor(schema.getElementType()), TREPEATED);
            }
        } else if (type.equals(Schema.Type.MAP)) {
            List<Schema.Field> keyValueRecord = Lists.newArrayList(
                new Schema.Field("key", Schema.create(Schema.Type.STRING), null, null),
                new Schema.Field("value", schema.getValueType(), null, null));
            return complex(fieldName, TRECORD, TREPEATED, convertFields(keyValueRecord));
        } else if (type.equals(Schema.Type.UNION)) {
            return convertUnion(fieldName, schema);
        }
        throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }

    private String convertUnion(String fieldName, Schema schema) {
        List<Schema> nonNullSchemas = new ArrayList(schema.getTypes().size());
        schema.getTypes().stream().filter((childSchema) -> (!childSchema.getType().equals(Schema.Type.NULL)))
            .forEach((childSchema) -> {
                nonNullSchemas.add(childSchema);
            });
        switch (nonNullSchemas.size()) {
            case 0:
                throw new UnsupportedOperationException("Cannot convert Avro union of only nulls");

            case 1:
                return convertField(fieldName, nonNullSchemas.get(0));

            default:
                return convertField(fieldName, nonNullSchemas.get(0));
        }
    }

    private String convertField(Schema.Field field) {
        return convertField(field.name(), field.schema());
    }

    private String field(String fieldName, String type) {
        return String.format("{\"name\" : \"%s\", \"type\" : \"%s\"}",
            fieldName, type);
    }

    private String field(String fieldName, String type, String mode) {
        return String.format("{\"name\" : \"%s\", \"type\" : \"%s\", \"mode\" : \"%s\"}",
            fieldName, type, mode);
    }

    private String complex(String fieldName, String type, String mode, String fields) {
        return String.format("{\"name\" : \"%s\", \"type\" : \"%s\", \"mode\" : \"%s\", \"fields\" : [%s]}",
            fieldName, type, mode, fields);
    }

    private String typeFor(Schema schema) {
        Schema.Type type = schema.getType();
        if (type.equals(Schema.Type.BOOLEAN)) {
            return TBOOLEAN;
        } else if (type.equals(Schema.Type.INT) || type.equals(Schema.Type.LONG)) {
            return TINTEGER;
        } else if (type.equals(Schema.Type.FLOAT) || type.equals(Schema.Type.DOUBLE)) {
            return TFLOAT;
        } else if (type.equals(Schema.Type.STRING) || type.equals(Schema.Type.ENUM)) {
            return TSTRING;
        } else if (type.equals(Schema.Type.RECORD)) {
            return TRECORD;
        } else if (type.equals(Schema.Type.BYTES)) {
            return TBYTES;
        } else if (type.equals(Schema.Type.UNION)) {
            for (Schema childSchema : schema.getTypes()) {
                if (!childSchema.getType().equals(Schema.Type.NULL)) {
                    return typeFor(childSchema);
                }
            }
        }
        throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Expected 1 argument: input_schema_file_avsc");
        }
        String convert = convert(args[0], new Configuration());
        System.out.println(convert);

    }

    public static Schema readSchema(String input, Configuration conf) throws IOException, IllegalArgumentException {
        Schema schema;
        if (input.startsWith("hdfs:") || input.startsWith("gs:") || input.startsWith("file:")) {
            FileSystem fs = FileSystem.get(conf);
            InputStream schemaStream = fs.open(new Path(input));
            schema = new Schema.Parser().parse(schemaStream);
        } else {
            schema = new Schema.Parser().parse(new File(input));
        }
        return schema;
    }

    public static String convert(String input, Configuration conf) throws IOException, IllegalArgumentException {
        return new SchemaConverter().convert(readSchema(input, conf));
    }
}
