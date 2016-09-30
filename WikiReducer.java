package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * This reducer method implemented by this class gets a key of type Text, which is basically a name,
 * and then a whole list of values (one or two), one meaning that the name came from the people file, 
 * and two meaning it came from the wiki file. that hadoop sent to the particular node. The method iterates over
 * all the values, and if it sees a one, then it knows the name is contained in the people file, so I set 
 * that flag to be true. Then, given that happening, I sum all the instances with value 2 to get the number
 * of times that the particular name was found in the wiki file. Then the reducer writes out a pair of
 * (NullWritable, Text) where the text is a string that i built with the number of times the name was counted
 * (assuming > 0), and then the key (the name).
 */
public class WikiReducer extends Reducer<Text, IntWritable, NullWritable, Text> {

	 public void reduce(Text key, Iterable<IntWritable> records, Context context) throws IOException, InterruptedException {
		 
		 String outputString = null;
		 Text output = null;
		 int instances = 0;
		 Boolean inPeople = false;
		 
		 // iterate over all IntWritable values passed here by the reducers.
		 for (IntWritable v : records) {
			 int intV = v.get();
			 // now see which file that name, value pair came from.
			 if (intV == 1) {
				 inPeople = true;
			 }
			 if (intV == 2) {
				 instances++;
			 }
		 }
		 
		 // construct output string
		 outputString = instances + " " + key.toString();
		 output = new Text(outputString);
		 
		 // if it is in the people file, and it appeared at least once in the wiki.
		 if (instances > 0 && inPeople) {
			 context.write(NullWritable.get(), output);
		 }
	 }
}

