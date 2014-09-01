package sogou.pingback.log.hadoop;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ProcessPvLog extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 3)
		{
			System.err.println("Usage: ProcessPvLog <pvlog file path> <output file path> <reduce number>");
			System.exit(-1);
		}
									
	    int ret = ToolRunner.run(new ProcessPvLog(), args);
	    System.exit(ret);
	}
	
	public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException
	{
		Job job = new Job(this.getConf());
	    job.setJarByClass(ProcessPvLog.class);
	        
	    job.setMapperClass(PvLogMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	                
	    job.setReducerClass(PvLogReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    int reduceNum = Integer.parseInt(args[2]);
	    job.setNumReduceTasks(reduceNum);
	    job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(GbkOutputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    TextInputFormat.setInputPaths(job, new Path(args[0]));
	    TextOutputFormat.setOutputPath(job, new Path(args[1]));
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 2;
	}

	
	public static class PvLogMapper extends Mapper<Object, Text, Text, Text>
	{	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			//Text newText = transformTextToUTF8(value, "gbk");
			//String line = newText.toString();
			String line = value.toString();
			line = line.trim();
			if(line.length() <= 0)
				return;
			
		    context.write(new Text("s"), new Text("s"));
		}
	}
	
	public static class PvLogReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{		
			context.write(new Text("s"), new Text("s"));
			
						
		}
		public void cleanup(Context context) throws IOException,InterruptedException 
		{
			
		}
		
	}

	public static Text transformTextToUTF8(Text text, String encoding) 
	{
		String value = null;
		try 
		{
		  value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} 
		catch (UnsupportedEncodingException e) 
		{
		  e.printStackTrace();
		}
		return new Text(value);
		
	}
	
}