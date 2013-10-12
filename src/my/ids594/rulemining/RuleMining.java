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
		
		//DEBUG
		String[] testinput = new String[3];
		testinput[0] = "/home/pavan/Desktop/input/file01space";
		testinput[1] = "/home/pavan/Desktop/temp";
		testinput[2] = "/home/pavan/Desktop/output";
		
		args = testinput;
		
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
	
		    itemsetsJob.setInputFormat(TextInputFormat.class);
		    itemsetsJob.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(itemsetsJob, new Path(arg0[0]));
		    FileOutputFormat.setOutputPath(itemsetsJob, new Path(arg0[1]));
	    
		    JobConf conf2 = new JobConf(RuleMining.class);
			conf2.setJobName("Association rules confidence computation");

			conf2.setOutputKeyClass(Text.class);
		    conf2.setOutputValueClass(Text.class);
		    
		    conf2.setMapperClass(ComputationMapper.class);
		    conf2.setReducerClass(ComputationReducer.class);
		    
		    conf2.setInputFormat(TextInputFormat.class);
		    conf2.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(conf2, new Path(arg0[1]));
		    FileOutputFormat.setOutputPath(conf2, new Path(arg0[2]));
		    
		    
			JobClient.runJob(itemsetsJob);
			JobClient.runJob(conf2);			
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return 0;		
	}	
}
