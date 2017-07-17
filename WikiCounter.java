import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*Main class to set further classes
 * receive arguments from command line
 * configure the job parameters
 * Implement run() function
 */

public class WikiCounter extends Configured implements Tool
{
	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration(getConf());
		
		conf.addResource(new Path("/local/bd4/bd4-hadoop-ug/conf/core-site.xml")); 
		
		conf.set("mapred.jar", "/users/msc/2255654s/WikiCounter.jar");  
		conf.set("argument2",args[2]);
		conf.set("argument3",args[3]);
		conf.set("argument4",args[4]);
		
	    Job job = Job.getInstance(conf);
	    
	    job.setJobName("WikiCounter");
	    job.setJarByClass(WikiCounter.class);
	    
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
	      
	    job.setInputFormatClass(TextInputFormat.class); 
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setNumReduceTasks(1);

	    FileInputFormat.setInputPaths(job, new Path(args[0]));    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	    return 0;
	
	}	
	
	
	public static void main(String[] args) throws Exception
	{
	   System.exit(ToolRunner.run(new WikiCounter(), args));
	   
	   
	}

}
