package sogou.pingback.log.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.LzoTextInputFormat;


public class SessionTrainData extends Configured implements Tool{

	public final static Pattern logPattern = Pattern.compile("\\[.*?\\]");
	public final static String sogouObserverStart = "Sogou-Observer";
	
	public static class LogMapper extends Mapper<Object, Text, Text, Text> {
		List<String> logColList = new ArrayList<String>();
		Matcher matcher;
		int count = 0;
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(value == null || value.toString().length()==0){
				count++;
				return;
			}
			
			String[] cols = value.toString().split("\t");
			String outkey = cols[1]+count;
			
			context.write(new Text(outkey), new Text(value));
		}
	}

	public static class LogReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{			
			
			
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception {
		if(args.length != 4)
		{
			System.err.println("Usage: ProcessLingxi <startdate> <enddate> <output file path> <reduce number>");
			System.exit(-1);
		}
									
	    int ret = ToolRunner.run(new SessionTrainData(), args);
	    System.exit(ret);
	}
	
	public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Job job = new Job(this.getConf());
	    job.setJarByClass(SessionTrainData.class);
	        
	    job.setMapperClass(LogMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
//	    ChainMapper.
	    
	    job.setReducerClass(LogReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    int reduceNum = Integer.parseInt(args[3]);
	    reduceNum = 0;
	    job.setNumReduceTasks(reduceNum);
	    
	    job.setInputFormatClass(LzoTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	     
	    List<String> directoryList = GeneratePath.genPathByDate(PingbackConstant.pingbackBasePath, args[0], args[1]);
	    for(String dir : directoryList){
	    	MultipleInputs.addInputPath(job, new Path(dir),LzoTextInputFormat.class);
	    }

	    TextOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 2;
	}


}
