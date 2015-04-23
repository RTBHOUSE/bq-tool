package com.rtbhouse.bq.avro;

import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineAvroKeyInputFormat<T> extends CombineFileInputFormat<AvroKey<T>, NullWritable> {

    @Override
    public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException {

        Class x = AvroKeyRecordReaderWrapper.class;

        return new CombineFileRecordReader<>(
                (CombineFileSplit) inputSplit,
                context,
                (Class<? extends RecordReader<AvroKey<T>, NullWritable>>) x);
    }

    public static class AvroKeyRecordReaderWrapper<T> extends CombineFileRecordReaderWrapper<AvroKey<T>, NullWritable> {

        public AvroKeyRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx)
                throws IOException, InterruptedException {

            super(new AvroKeyInputFormat<>(), split, context, idx);
        }
    }
}
