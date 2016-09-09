package org.broadinstitute.hellbender.engine.spark.datasources;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.VCFFileMerger;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;

/**
 * VariantsSparkSink writes variants to a VCF file in parallel using Hadoop-BAM.
 */
public final class VariantsSparkSink {

    // Output format class for writing VCF files through saveAsNewAPIHadoopFile. Must be public.
    public static class SparkVCFOutputFormat extends KeyIgnoringVCFOutputFormat<NullWritable> {
        public static VCFHeader vcfHeader;

        public static void setVCFHeader(final VCFHeader header) {
            vcfHeader = header;
        }

        public SparkVCFOutputFormat() {
            super(VCFFormat.VCF);
        }

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
            setHeader(vcfHeader);
            // don't add an extension, since FileOutputFormat will add a compression extension automatically (e.g. .bgz)
            return super.getRecordWriter(ctx);
        }

        @Override
        public void checkOutputSpecs(JobContext job) throws IOException {
            try {
                super.checkOutputSpecs(job);
            } catch (FileAlreadyExistsException e) {
                // delete existing files before overwriting them
                final Path outDir = getOutputPath(job);
                outDir.getFileSystem(job.getConfiguration()).delete(outDir, true);
            }
        }
    }

    public static class SparkHeaderlessVCFOutputFormat extends SparkVCFOutputFormat {
        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
            ctx.getConfiguration().setBoolean(WRITE_HEADER_PROPERTY, false);
            return super.getRecordWriter(ctx);
        }
    }

    /**
     * Write variants to the given output file in VCF format with the given header. Note that writing sharded output is not supported.
     * @param ctx the JavaSparkContext
     * @param outputFile path to the output VCF
     * @param variants variants to write
     * @param header the header to put at the top of the output file
     * @throws IOException if an error occurs while writing
     */
    public static void writeVariants(
            final JavaSparkContext ctx, final String outputFile, final JavaRDD<VariantContext> variants,
            final VCFHeader header) throws IOException {
        String absoluteOutputFile = makeFilePathAbsolute(outputFile);
        writeVariantsSingle(ctx, absoluteOutputFile, variants, header);
    }

    private static void writeVariantsSingle(
            final JavaSparkContext ctx, final String outputFile, final JavaRDD<VariantContext> variants,
            final VCFHeader header) throws IOException {

        final Configuration conf = ctx.hadoopConfiguration();
        if (outputFile.endsWith(BGZFCodec.DEFAULT_EXTENSION)) {
            conf.setBoolean(FileOutputFormat.COMPRESS, true);
            conf.setClass(FileOutputFormat.COMPRESS_CODEC, BGZFCodec.class, CompressionCodec.class);
        } else {
            conf.setBoolean(FileOutputFormat.COMPRESS, false);
        }

        final String outputPartsDirectory = outputFile + ".parts";
        saveAsShardedHadoopFiles(ctx, conf, outputPartsDirectory, variants,  header, false);
        VCFFileMerger.mergeParts(outputPartsDirectory, outputFile, header);
    }

    private static void saveAsShardedHadoopFiles(
            final JavaSparkContext ctx, final Configuration conf, final String outputFile, JavaRDD<VariantContext> variants,
            final VCFHeader header, final boolean writeHeader) throws IOException {
        // Set the static header on the driver thread.
        SparkVCFOutputFormat.setVCFHeader(header);

        final Broadcast<VCFHeader> headerBroadcast = ctx.broadcast(header);

        // SparkVCFOutputFormat is a static class, so we need to copy the header to each worker then call
        final JavaRDD<VariantContext> variantsRDD = setHeaderForEachPartition(variants, headerBroadcast);

        // The expected format for writing is JavaPairRDD where the key is ignored and the value is VariantContextWritable.
        final JavaPairRDD<VariantContext, VariantContextWritable> rddVariantContextWriteable = pairVariantsWithVariantContextWritables(variantsRDD);

        rddVariantContextWriteable.saveAsNewAPIHadoopFile(outputFile, VariantContext.class, VariantContextWritable.class, getOutputFormat(writeHeader), conf);
    }

    private static JavaRDD<VariantContext> setHeaderForEachPartition(final JavaRDD<VariantContext> variants, final Broadcast<VCFHeader> headerBroadcast) {
        return variants.mapPartitions(readIterator -> {
            SparkVCFOutputFormat.setVCFHeader(headerBroadcast.getValue());
            return new IteratorIterable<>(readIterator);
        });
    }

    private static JavaPairRDD<VariantContext, VariantContextWritable> pairVariantsWithVariantContextWritables(JavaRDD<VariantContext> records) {
        return records.mapToPair(variantContext -> {
            final VariantContextWritable variantContextWritable = new VariantContextWritable();
            variantContextWritable.set(variantContext);
            return new Tuple2<>(variantContext, variantContextWritable);
        });
    }

    private static Class<? extends OutputFormat<NullWritable, VariantContextWritable>> getOutputFormat(final boolean writeHeader) {
        return writeHeader ? SparkVCFOutputFormat.class : SparkHeaderlessVCFOutputFormat.class;
    }

    private static String makeFilePathAbsolute(String path){
        if(BucketUtils.isCloudStorageUrl(path) || BucketUtils.isHadoopUrl(path) || BucketUtils.isFileUrl(path)){
            return path;
        } else {
            return new File(path).getAbsolutePath();
        }
    }
}
