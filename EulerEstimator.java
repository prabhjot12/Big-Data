import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Locale;
import java.text.BreakIterator;
import java.text.Normalizer;
import java.util.Random;




public class EulerEstimator extends Configured implements Tool{

	 
    public static class EulerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable>{
 

        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        		long iterations_counter=0;
        		long count_counter=0;
        		long iter = Integer.parseInt(value.toString());
        		long count = 0;
    			context.getInputSplit();
    			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        		Random random=new Random(fileName.hashCode()+key.get());
        			for(long i=1;i<=iter;i++)
        			{
        			    double sum = 0.0;
        			    while(sum < 1)
        			    {
        			        sum += random.nextDouble();
        			        count++;
        			    }    			    
        			}
        			iterations_counter += iter;
        			count_counter += count;
        			context.getCounter("Euler", "iterations").increment(iter);
        			context.getCounter("Euler", "count").increment(count);

        }
    } 
    

   

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
        System.exit(res);
    }
 
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(EulerEstimator.class);
 
        job.setInputFormatClass(TextInputFormat.class);
 
        job.setMapperClass(EulerMapper.class);
//      job.setCombinerClass(EulerMapper.class);
//      job.setReducerClass(EulerMapper.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
//        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
