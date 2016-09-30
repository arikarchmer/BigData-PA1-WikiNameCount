package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * This simple PeopleMapper class implements the map method that takes a key (name),
 * and then assigns it an IntWritable value of one, so that later when the pair is passed 
 * to the reducer class it will know that it came from the people file.
 */
public class PeopleMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		IntWritable one = new IntWritable(1);

		context.write(value, one);
	}
}
