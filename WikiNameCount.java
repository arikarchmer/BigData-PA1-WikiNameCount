package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;


/*
 * Main class. The main method sets up the jobs. First, a job is created with a configuration instance.
 * Then the I let hadoop know that 'WikiNameCount.class' will be the main class. Then, I set the map
 * output as a key value pair (k, v) with types (Text, IntWritable). The job will have multiple inputs
 * (wiki file and people.txt), so I set the first parameter to be the wiki, and the second to be the people
 * by telling hadoop to use the wikimapper then the peoplemapper respectively. Finally I set the 
 * reducer to output a pair of (NullWritable, Text), and set the output path for hadoop in the hdfs.
 */
public class WikiNameCount extends Configured {
	
	public static void main(String[] arguments) throws Exception{                               
	    Configuration conf = new Configuration();
	  
	    String[] args = new GenericOptionsParser(conf, arguments).getRemainingArgs();
		
	    Job job = Job.getInstance(conf);
	    job.setJarByClass(WikiNameCount.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WikiMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PeopleMapper.class);
	                              
	    job.setReducerClass(WikiReducer.class);             
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(arguments[2]));
	    boolean status = job.waitForCompletion(true);
	    
	    System.exit(status ? 0 : 1);  
	}
	
	// took 41 seconds to run!!!
}

