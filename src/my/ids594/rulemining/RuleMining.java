package my.ids594.rulemining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class RuleMining {

	public static void main(String[] args) {
		
		
		try {
			JobConf conf = new JobConf(RuleMining.class);
			conf.setJobName("Association Rule Mining");
			
			conf.setOutputKeyClass(Text.class);
		    conf.setOutputValueClass(IntWritable.class);
			
			conf.setMapperClass(Map.class);
		    conf.setReducerClass(Reduce.class);
	
		    conf.setInputFormat(TextInputFormat.class);
		    conf.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
			JobClient.runJob(conf);
			
		} catch (IOException e) {

			e.printStackTrace();
		}
		catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private Text itemset = new Text();
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> im_output, Reporter reporter)
				throws IOException {
			
			String transaction = value.toString();
			List<String> itemsets = getItemSets(transaction.split(","));
			
			for(String itmset : itemsets){				
				itemset.set(itmset);
				im_output.collect(itemset, new IntWritable(1));
			}			
		}

		private List<String> getItemSets(String[] items) {
			
			List<String> itemsets = new ArrayList<String>();	
			
			int n = items.length;
			
			int[] masks = new int[n];
			
			for (int i = 0; i < n; i++)
	            masks[i] = (1 << i);
			
			for (int i = 0; i < (1 << n); i++){				
				
				String x = "(";  //List<String> newList = new ArrayList<String>(n);
				for (int j = 0; j < n; j++){
	                if ((masks[j] & i) != 0){
	                	
	                	x = x + items[j] + ",";  //newList.add(items[j]);
	                }
	                
	                if(j == n-1){
	                	x = x.substring(0,x.length()-1) + ")";
	                	if(x != ")")	                	
	                		itemsets.add(x);
	                }
				}
			}
	        			
			return itemsets;
		}		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			int sum = 0;
		       	while (values.hasNext()) {
		       		sum += values.next().get();
		       }
		       output.collect(key, new IntWritable(sum));
		}			
	}		
}
