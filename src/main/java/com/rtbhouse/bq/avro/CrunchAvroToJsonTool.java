package com.rtbhouse.bq.avro;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.Charsets;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class CrunchAvroToJsonTool extends Configured implements Tool, Serializable {

    public static final String JOB_NAME = CrunchAvroToJsonTool.class.getSimpleName();

    public static final String SCHEMA_BQSC_FILE = "schema.bqsc";

    public static final int MAXSIZE = 2 * 1024 * 1024;

    private static Schema schema = null;

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CrunchAvroToJsonTool(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        parseConfig(args);
        Configuration conf = getConf();
        schema = SchemaConverter.readSchema(avroschema, conf);
        String convert = new SchemaConverter().convert(schema);
        Files.write(convert, new File(SCHEMA_BQSC_FILE), Charsets.UTF_8);
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
        conf.setInt("crunch.combine.file.block.size", maxMapSizeMb * 1024 * 1024);
        Pipeline pipeline = new MRPipeline(getClass(), getClass().getSimpleName(), conf);
        List<Path> inputs = new ArrayList<>();
        Lists.newArrayList(files.split(",")).stream().forEach((file) -> {
            inputs.add(new Path(file));
        });
        PCollection<Record> avroRecords = pipeline.read(From.avroFile(inputs, Avros.generics(schema)));
        avroRecords
            .parallelDo(new DoFn<Record, String>() {
                private int skipped = 0;

                @Override
                public void process(Record avro, Emitter<String> emtr) {
                    String build = new JsonBuilder().build(avro);
                    int length = build.length();
                    if (length > maxRowJsonSize) {
                        System.out.println(String.format("Skipping record too big: %s; total skipped: %s", length, ++skipped));
                        return;
                    }
                    emtr.emit(build);
                }

            }, Writables.strings())
            .write(To.textFile(outputDirectory));
        pipeline.done();
        return 0;
    }

    String avroschema;

    String files;

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
            files = cl.getOptionValue("f");
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
        String syntax = CrunchAvroToJsonTool.class.getName() + " [<options>]";
        formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
        System.out.println(sw.toString());
    }
}
