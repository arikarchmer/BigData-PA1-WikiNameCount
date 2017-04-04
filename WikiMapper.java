package mapreduce;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * This WikiMapper class is more complicated than the PeopleMapper. First of all, it assumes
 * that all the names in the people.txt file are two tokens. Big simplification, I know,
 * but it had to be done (I was pressed for time). It implements the map method,
 * which gets the Text values, which is a line from the file, then tokenizes the line.
 * In the while loop, every subset of two consecutive tokens is examined, and then an output 
 * is constructed by creating a potential name out of the two tokens. This potential name
 * is paired with an IntWritable value of two, so that the reducer will then know this name 
 * came from the wiki file. This pair is then written to the Context object.
 */
public class WikiMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
		
		IntWritable two = new IntWritable(2);
		
		String[] potential_name = new String[2];
		int first = 0;
		int second = 1;
		
		String v = values.toString();
	
		StringTokenizer tokenizer = new StringTokenizer(v);
		
		while (tokenizer.hasMoreTokens()) {
			
			// here is every subset of two consecutive tokens.
			if (potential_name[second] != null) {
				potential_name[first] = potential_name[second];
			} else {
				potential_name[first] = tokenizer.nextToken();
			}
			if (tokenizer.hasMoreTokens()) {
				potential_name[second] = tokenizer.nextToken();
			}
		}
		
		// create potential name
		String potentialName = potential_name[first] + " " + potential_name[second];
		Text pNameText = new Text(potentialName);

        context.write(pNameText, two);
	}
}