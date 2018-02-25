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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Locale;
import java.text.BreakIterator;
import java.text.Normalizer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class RedditAverage extends Configured implements Tool{

	public static class TokenizerMapper
    extends Mapper<LongWritable, Text, Text, LongPairWritable>{
 
        private final static LongPairWritable val = new LongPairWritable();
        
        
        private final static LongWritable one = new LongWritable(1);
        
        private final static LongWritable commentScore = new LongWritable();
        
        private Text word = new Text();
        
 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
        	ObjectMapper json_mapper = new ObjectMapper();
        	
        	String input=value.toString();
        	 
        	JsonNode data = json_mapper.readValue(input, JsonNode.class);
        	      	
        	word.set(data.get("subreddit").textValue());
        	
        	commentScore.set(data.get("score").longValue());
        	
        	val.set(one.get(),commentScore.get());
        	
        	context.write(word,val);

        }
	}
	
    public static class AverageCombiner
    extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
        private LongPairWritable result = new LongPairWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long sum = 0;
            long frequency=0;
            for (LongPairWritable val : values) {
                sum += val.get_1();
                frequency+=val.get_0();
            }

            result.set(frequency,sum);
            context.write(key, result);
        }
    }
	
	
    public static class AverageReducer
    extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            long sum = 0;
            long frequency=0;
            for (LongPairWritable val : values) {
                sum += val.get_1();
                frequency+=val.get_0();
            }
            
            double final_avg=(double)sum/frequency;
            result.set(final_avg);
            context.write(key, result);
        }
    }
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }
    
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(RedditAverage.class);
 
        job.setInputFormatClass(MultiLineJSONInputFormat.class);
 
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AverageCombiner.class);
        job.setReducerClass(AverageReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
