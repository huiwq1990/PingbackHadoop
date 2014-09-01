import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SSs {

	public static void main(String[] args) {
//		String 

		
		// TODO Auto-generated method stub

		
		
		map();

	}

	public static void map() {
		String line = "[INFO]  [20140730 00:00:05] [7F907466C700] [Sogou-Observer,type=7,ks=6,assn=0,h=358239056386818,v=6.1.1,rid=10,hs=0,shum=,snum=,inum=,vnum=,jnum=,wt=,cpos=,ctype=,swd=,ft=1,pname=com.taobao.taobao,req=8,esp=%E8%BF%90%E5%8A%A8,asp=,can=%E4%BB%96,candt=2,lbs=gsm_460_01_46493_46214715%3B,cwd=,rd=,cost=237,missn=20,to=500,pro=2,pl=,bt=0,is=1,t=15,ename=%E6%90%9C%E7%B4%A2,cu=,ed=0,lx=0,close=-1,Owner=OP]";
		line = line.trim();
		if (line == ""){
			return;
		}
		
		Pattern pattern = Pattern.compile("\\[.*?\\]");
		Matcher matcher = pattern.matcher(line);

		List<String> logColList = new ArrayList<String>();
		
		 while (matcher.find()) {// 遍历找到的所有大括号
		       String param = matcher.group().replaceAll("\\[", "").replaceAll("\\]", "");// 去掉括号
			 System.out.println(param);
			 logColList.add(param);
		 }
		 
		 
		 if(logColList.size()<3){
			 return;
		 }
		 
		 
	}
}
