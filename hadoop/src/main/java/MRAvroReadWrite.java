import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by liju on 6/5/17.
 *
 * MapReduce example to read avro input and write Avro output
 *
 * Calculate avg mark of each student
 */
public class MRAvroReadWrite extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        final int result = ToolRunner.run(new MRAvroReadWrite(), args);
        System.exit(result);
    }

    /* Mapper class */
    public static class AvgCalMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,IntWritable,IntPair>{
        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            final Integer student_id = (Integer) key.datum().get("student_id");
            final Integer marks = (Integer) key.datum().get("marks");
            context.write(new IntWritable(student_id),new IntPair(marks,1));
        }
    }

    /* Combiner class */
    public static class AvgCalCombiner extends Reducer<IntWritable,IntPair,IntWritable,IntPair>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int freq=0;
            for (IntPair value : values) {
                sum+=value.getFirst();
                freq+=value.getSecond();
            }

            context.write(key,new IntPair(sum,freq));
        }
    }

    /* Reducer class */
    public static class AvgCalReducer extends Reducer<IntWritable,IntPair,AvroWrapper<Pair<Integer,Float>>,NullWritable>{

        @Override
        protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            float avg =0;
            int sum =0;
            int freq = 0;
            for (IntPair value : values) {
                sum+=value.getFirst();
                freq+=value.getSecond();
            }
            avg=(float) sum/freq;

            context.write(new AvroWrapper<Pair<Integer, Float>>(new Pair<Integer, Float>(key.get(),avg)),NullWritable.get());

        }
    }


    /**
     * Driver method
     * @param args : /Users/liju/Documents/gitprojects/bigdatastuff/hadoop/src/main/resources/student.avro /tmp/out
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        final Job job = Job.getInstance(getConf(),"AverageCalMRJob");
        job.setJarByClass(this.getClass());

        Path in  = new Path(args[0]);
        Path out  = new Path(args[1]);

        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        out.getFileSystem(getConf()).delete(out,true);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(AvgCalMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntPair.class);

        job.setCombinerClass(AvgCalCombiner.class);
        AvroJob.setOutputKeySchema(job, Pair.getPairSchema(Schema.create(Schema.Type.INT),Schema.create(Schema.Type.FLOAT)));
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(AvgCalReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }

}


