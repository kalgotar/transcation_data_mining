package my.ids594.rulemining;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RuleMining  extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
				
		int res = ToolRunner.run(new RuleMining(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		try{
			
			JobConf itemsetsJob = new JobConf(RuleMining.class);
			itemsetsJob.setJobName("Frequent Itemset generation");
			
			itemsetsJob.setOutputKeyClass(Text.class);
		    itemsetsJob.setOutputValueClass(IntWritable.class);
			
			itemsetsJob.setMapperClass(FrequentItemsetMapper.class);
		    itemsetsJob.setReducerClass(FrequentItemsetReducer.class);
		    itemsetsJob.setPartitionerClass(FrequentItemsetPartitioner.class);
		    
		    itemsetsJob.setNumReduceTasks(3); //Each for itemsets of size 1, 2 and 3 
		    
		    itemsetsJob.setInputFormat(TextInputFormat.class);
		    itemsetsJob.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(itemsetsJob, new Path(arg0[0]));
		    FileOutputFormat.setOutputPath(itemsetsJob, new Path(arg0[1]));
	    
		    JobConf computationJob = new JobConf(RuleMining.class);
			computationJob.setJobName("Association rules confidence computation");

			computationJob.setOutputKeyClass(Text.class);
		    computationJob.setOutputValueClass(Text.class);
		    
		    computationJob.setMapperClass(ComputationMapper.class);
		    computationJob.setReducerClass(ComputationReducer.class);
		    
		    computationJob.setInputFormat(TextInputFormat.class);
		    computationJob.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(computationJob, new Path(arg0[1]));
		    FileOutputFormat.setOutputPath(computationJob, new Path(arg0[2]));
		    		    
			JobClient.runJob(itemsetsJob);
			JobClient.runJob(computationJob);			
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return 0;		
	}	
}
