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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PingbackLogMR extends Configured implements Tool{

	Pattern logPattern = Pattern.compile("\\[.*?\\]");
	String sogouObserverStart = "Sogou-Observer";
	class LogMapper extends Mapper<Object, Text, Text, Text> {
		List<String> logColList = new ArrayList<String>();
		Matcher matcher;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Text newText = transformTextToUTF8(value, "gbk");
			// String line = newText.toString();
			String line = value.toString();
			line = line.trim();
			if (line.length() <= 0) {
				return;
			}

			matcher = logPattern.matcher(line);
			logColList.clear();
			while (matcher.find()) {
				String param = matcher.group().replaceAll("\\[", "")
						.replaceAll("\\]", "");//remove [ and ]
				logColList.add(param);
			}
			if (logColList.size() < 3 || logColList.get(2).startsWith(sogouObserverStart) == false) {
				return;
			}
			context.write(new Text(key.toString()), new Text(logColList.get(2)));
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3)
		{
			System.err.println("Usage: ProcessLingxi <startdate> <enddate> <output file path> <reduce number>");
			System.exit(-1);
		}
									
	    int ret = ToolRunner.run(new PingbackLogMR(), args);
	    System.exit(ret);
	}
	
	public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Job job = new Job(this.getConf());
	    job.setJarByClass(PingbackLogMR.class);
	        
	    job.setMapperClass(LogMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	                
//	    job.setReducerClass(PvLogReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    int reduceNum = Integer.parseInt(args[2]);
	    job.setNumReduceTasks(reduceNum);
	    job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(GbkOutputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	     
	    List<String> directoryList = GeneratePath.genPathByDate(PingbackConstant.pingbackBasePath, args[0], args[1]);
	    for(String dir : directoryList){
	    	MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class);
	    }

	    TextOutputFormat.setOutputPath(job, new Path(args[1]));
	    boolean success = job.waitForCompletion(true);
	    return success ? 0 : 2;
	}


}