package my.ids594.rulemining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
			JobConf conf1 = new JobConf(RuleMining.class);
			conf1.setJobName("Association Rule Mining - Mapred1");
			
			conf1.setOutputKeyClass(Text.class);
		    conf1.setOutputValueClass(IntWritable.class);
			
			conf1.setMapperClass(Map.class);
		    conf1.setReducerClass(Reduce.class);
	
		    conf1.setInputFormat(TextInputFormat.class);
		    conf1.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		    FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
	    
		    JobConf conf2 = new JobConf(RuleMining.class);
			conf2.setJobName("Association Rule Mining - Mapred2");

			conf2.setOutputKeyClass(Text.class);
		    conf2.setOutputValueClass(Text.class);
		    
		    conf2.setMapperClass(Map2.class);
		    conf2.setReducerClass(Reduce2.class);
		    
		    conf2.setInputFormat(TextInputFormat.class);
		    conf2.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(conf2, new Path(args[1]));
		    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		    
		    
			JobClient.runJob(conf1);
			JobClient.runJob(conf2);
			
		} catch (Exception e) {
			
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
				itemset.set(itmset.replaceAll(" ", ""));
				im_output.collect(itemset, new IntWritable(1));
			}			
		}

		//getting powersets (exclusing empty set)
		private List<String> getItemSets(String[] items) {
			
			List<String> itemsets = new ArrayList<String>();	
			
			int n = items.length;
			
			int[] masks = new int[n];
			
			for (int i = 0; i < n; i++)
	            masks[i] = (1 << i);
			
			for (int i = 0; i < (1 << n); i++){				
				
				List<String> newList = new ArrayList<String>(n);
				
				for (int j = 0; j < n; j++){
	                if ((masks[j] & i) != 0){
	                	
	                	newList.add(items[j]);
	                }
	                
	                if(j == n-1 && newList.size() > 0){
              		
	                	itemsets.add(newList.toString());
	                }
				}
			}
	        			
			return itemsets;
		}		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

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
	
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		private Text itemset = new Text();
		private Text val = new Text();
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> im_output, Reporter reporter)
				throws IOException {

			String[] valueSplit = value.toString().split("\\t");
			
			itemset.set(valueSplit[0].replaceAll(" ", ""));	//[1,2,3]
			val.set("0;" + valueSplit[1]); //2
			
			//original itemset
			im_output.collect(itemset, val);
			
			String[] items = valueSplit[0].replace("[", "").replace("]", "").split(",");
			
			if(items.length > 1){
			
				List<String> subitemsets = getItemSets(items);
				
				for(String itmset : subitemsets){
					
					itemset.set(itmset.replaceAll(" ", ""));
					val.set(valueSplit[0] + ";" + valueSplit[1]);
					im_output.collect(itemset, val);
				}
			}
		}

		private List<String> getItemSets(String[] items) {
			
			List<String> itemsets = new ArrayList<String>();		
			int n = items.length;			
			int[] masks = new int[n];
			
			for (int i = 0; i < n; i++)
	            masks[i] = (1 << i);
			
			for (int i = 0; i < (1 << n); i++){				
				List<String> newList = new ArrayList<String>(n);
				for (int j = 0; j < n; j++){
	                if ((masks[j] & i) != 0){
	                	newList.add(items[j]);
	                }
	                
	                if(j == n-1 && newList.size() == n-1){	              		
	                	itemsets.add(newList.toString());
	                }
				}				
			} 
			
			return itemsets;
		}
	}
	
	public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
				
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			int keyCount = 0;
			HashMap<String,Integer> hashMap = new HashMap<String,Integer>();
			
			String[] keyitems = key.toString().replace("[", "").replace("]", "").split(",");
				      
			while (values.hasNext()) {

				String val = values.next().toString();
				String[] valuesplit = val.split(";");   //[1,2,3];0 OR 0;1
				
	       		if(valuesplit[0].equals("0")){	       			
	       			keyCount = Integer.parseInt(valuesplit[1]);
	       		}
	       		else{
	       			String[] parentItems = valuesplit[0].replace("[", "").replace("]", "").split(",");
	       			hashMap.put(key + " -> " + getSeparateItem(parentItems,keyitems),  Integer.parseInt(valuesplit[1]));
	       		}	       		
			}
			
			Iterator iterator = hashMap.keySet().iterator();
		       
	        while(iterator.hasNext()){
	        	String k = iterator.next().toString();
	        	int v = hashMap.get(k);
	        	
	        	output.collect(new Text(k.replace("[", "{").replace("]", "}")), new FloatWritable(v/(float)keyCount));
	        }
		}

		private String getSeparateItem(String[] parentItems, String[] keyitems) {
			
			String item = null;
			List<String> items = Arrays.asList(keyitems);
			
			for(String s : parentItems){
				if(!items.contains(s)){
					item = s;
					break;
				}
			}				
			return item;
		}			
	}
}
