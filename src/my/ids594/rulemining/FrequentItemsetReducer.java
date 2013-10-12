package my.ids594.rulemining;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FrequentItemsetReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		int sum = 0;
	    while (values.hasNext()) {
	    	sum += values.next().get();
	    }		    
	    
	    output.collect(key, new Text(Integer.toString(sum)));
	}
}
