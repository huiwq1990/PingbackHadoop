
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
			
			//pv日志格式(ip,time,suid,query,refer,rn,stype,pid,htn,qcn,url...)
			String[] items = line.split("\t");
			if(items.length < 15)
				return;
			
			String ip = items[0].trim();
			if(ip.length() <= 0)
				return;

			String suid = items[2].trim();
			if(suid.length() <= 0)
				return;
			
			String query = items[3].trim();
			if(query.length() <= 0 || query.length() > 64)
				return;
			query = query.toLowerCase();
			
			//获取rn并转换为整形
			String rn = items[5].trim();
			long result = 0;
			Pattern pattern = Pattern.compile("[0-9]*"); 
		    if(pattern.matcher(rn).matches()) 
			    result = Long.parseLong(rn);
			else
				return;
		
		    //stype
		    String sstype = items[6].trim();
		    int stype = 0;
		    if(sstype.length() <= 0)
		    	return;
		    if(pattern.matcher(sstype).matches()) 
			    stype = Integer.parseInt(sstype);
			else
				return;
		    if(stype < 0 || stype > 3)
				return;
		    
		    //pid
		    String pid = items[7].trim();
		    if(pid.length() <= 0)
		    	return;
		    if(pid.length() > 28)
		    	pid = pid.substring(0, 27);
		    
		    //获取url,矫正stype
		    String url = "";
		    if(items.length >= 11)
		    	url = items[10].trim();
		    if(stype == 2 && pid.equals("sogou-site-a5912d5771cbddba"))
		    {
		    	String key1 = "sourceid=";
		    	int pos = url.indexOf(key1);
		    	if(pos == -1)
		    		stype = 0;
		    }
		    
		    //获取refer，矫正stype
		    String refer = items[4].trim();
		    if(refer.length() <= 0)
		    	return;
		    if(stype == 0 && refer.startsWith("http://www.sohu.com"))
		    	stype = 2;
		    
		    String yyid = items[13].trim();
		    if(yyid.length() <= 0)
		    	yyid = "null";
		    String suv = items[14];
		    if(suv.length() <= 0)
		    	suv = "null";
		    	
		    String res = query+"\t"+suid+"\t"+result+"\t"+stype+"\t"+yyid
		    		     +"\t"+suv;
		    context.write(new Text(query), new Text(res));
		}
	}
	
	public static class PvLogReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{			
			for (Text v : values) 
			{
				String line = v.toString();
				if(line.length() <= 0)
					continue;
				//日志格式(ip, suid, query, stype)
				String[] items = line.split("\t");
				if(items.length < 4)
					continue;
				
				//query
				query = items[0];
				if(query.length() <= 0 || query.length() > 64)
					return;
				
				//suid
				String suid = items[1];				
				
				//rn
				Pattern pattern = Pattern.compile("[0-9]*"); 
				String sresult = items[2];
				long result = 0;
				if(pattern.matcher(sresult).matches()) 
			        result = Long.parseLong(sresult);
				else
					continue;
				
				//stype
				String sstype = items[3].trim();
				int stype = 0;
				if(pattern.matcher(sstype).matches()) 
					stype = Integer.parseInt(sstype);
				else
					continue;
				
				validnumber += 1;
				if(validnumber == 1)
					lastquery = query;
				if(!lastquery.equals(query))
				{
					for(String keyValue : suids.keySet())
					{
						int _stype = suids.get(keyValue);
						count[_stype+3] += 1;
					}
					String res = count[0]+"\t"+count[1]+"\t"+count[2]+"\t"+count[3]+"\t"+
							count[4]+"\t"+count[5]+"\t"+count[6];
					//System.out.println(lastquery+"\t"+res);				
					if(lastquery.length() > 0)
					    context.write(new Text(lastquery), new Text(res));
					
				    suids.clear();
				    for(int i=0; i<=6; ++i)
				        count[i] = 0;
				}
				
				lastquery = query;
				
				if(suids.isEmpty() || !suids.containsKey(suid))
				{     
					suids.put(suid, 0);
					count[2] += 1;
				}
				
				if(result > count[0])
					count[0] = result;
				count[1] += 1;
				
				if(suids.get(suid) != 0)
					continue;
				else if(stype != 0)
				{
					suids.remove(suid);
					suids.put(suid, stype);
				}
			}				
		}
		public void cleanup(Context context) throws IOException,InterruptedException 
		{
			for(String keyValue : suids.keySet())
			{
				int _stype = suids.get(keyValue);
				count[_stype+3] += 1;
			}
			String res = count[0]+"\t"+count[1]+"\t"+count[2]+"\t"+count[3]+"\t"+
					     count[4]+"\t"+count[5]+"\t"+count[6];
	    
			//System.out.println(query+"\t"+res);
			if(query.length() > 0)
		        context.write(new Text(query), new Text(res));	
		}
		private HashMap<String, Integer> suids = new HashMap<String, Integer>();
		private int validnumber = 0;
		private String lastquery = "";
		private String query = "";
		private long[] count = {0,0,0,0,0,0,0};
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