package com.rtbhouse.bq.avro;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.io.Files;

public class AvroToJson extends Configured implements Tool {

    public static final String JOB_NAME = AvroToJson.class.getSimpleName();

    public static final String ROWMAXSIZE = "row.maxsize";

    public static final String SCHEMA_BQSC_FILE = "schema.bqsc";

    public static final int MAXSIZE = 2 * 1024 * 1024;

    private static Schema schema = null;

    public static class GenericRecordMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, NullWritable> {

        private final Text text = new Text();

        private int skipped = 0;

        private int rowMaxSize;

        @Override
        public void setup(Context context) throws IOException {
            Configuration configuration = context.getConfiguration();
            rowMaxSize = configuration.getInt(ROWMAXSIZE, MAXSIZE);
        }

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
            throws IOException, InterruptedException {

            GenericRecord avro = key.datum();

            String build = new JsonBuilder().build(avro);
            int length = build.length();
            if (length > rowMaxSize) {
                System.out.println(String.format("Skipping record too big: %s; total skipped: %s", length, ++skipped));
                return;
            }

            text.set(build);
            context.write(text, NullWritable.get());
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = createJob(args);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public Job createJob(String[] args) throws Exception {
        parseConfig(args);
        Path output = new Path(outputDirectory);
        Configuration conf = getConf();
        conf = conf == null ? new Configuration() : conf;
        conf.set(ROWMAXSIZE, String.valueOf(maxRowJsonSize));
        Job job = Job.getInstance(conf);
        job.setJobName(JOB_NAME);
        job.setJarByClass(AvroToJson.class);
        job.setMapperClass(GenericRecordMapper.class);
        job.setNumReduceTasks(0);

        schema = SchemaConverter.readSchema(avroschema, conf);
        String convert = new SchemaConverter().convert(schema);
        Files.write(convert, new File(SCHEMA_BQSC_FILE), Charsets.UTF_8);
        AvroJob.setInputKeySchema(job, schema);
        job.setInputFormatClass(CombineAvroKeyInputFormat.class);
        CombineAvroKeyInputFormat.setMaxInputSplitSize(job, maxMapSizeMb * 1024 * 1024L);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        for (String inputEntry : file.split(",")) {
            FileInputFormat.addInputPath(job, new Path(inputEntry));
        }
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvroToJson(), args);
        System.exit(res);
    }

    String avroschema;

    String file;

    String outputDirectory;

    int maxMapSizeMb;

    int maxRowJsonSize;

    private void parseConfig(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("s", "avroschema", true, "Avro schema file to be processed.");
        options.addOption("f", "file", true, "Avro file or directory to be processed.");
        options.addOption("o", "output", true, "Output directory.");
        options.addOption("m", "mapsize", true, "Max split mapsize in MB.");
        options.addOption("r", "rowsize", true, "Max json row size bytes.");
        options.addOption("u", "usage", false, "Print usage.");

        CommandLineParser parser = new PosixParser();
        CommandLine cl = parser.parse(options, args, false);

        if (args.length == 0 || cl.hasOption("u")) {
            logUsage(options);
            throw new ParseException("u");
        }

        if (cl.hasOption("s")) {
            avroschema = cl.getOptionValue("s");
        } else {
            logUsage(options);
            throw new ParseException("s");
        }

        if (cl.hasOption("f")) {
            file = cl.getOptionValue("f");
        } else {
            logUsage(options);
            throw new ParseException("f");
        }

        if (cl.hasOption("o")) {
            outputDirectory = cl.getOptionValue("o");
        } else {
            logUsage(options);
            throw new ParseException("o");
        }

        if (cl.hasOption("m")) {
            maxMapSizeMb = Integer.parseInt(cl.getOptionValue("m"));
        } else {
            maxMapSizeMb = 512;
        }

        if (cl.hasOption("r")) {
            maxRowJsonSize = Integer.parseInt(cl.getOptionValue("r"));
        } else {
            maxRowJsonSize = MAXSIZE;
        }

    }

    private static void logUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String syntax = AvroToJson.class.getName() + " [<options>]";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
    }
}
