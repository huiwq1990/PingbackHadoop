package sogou.pingback.log.hadoop;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import sogou.pingback.util.DateUtil;

public class GeneratePath {
	
	
	public static List<String> genPathByDate(String basepath,String startDateStr,String endDateStr){
//		String startDateStr = "2014-07-11";
//		String endDateStr = "2014-07-22";

		Date startDate = DateUtil.parseDate(startDateStr);
		Date endDate = DateUtil.parseDate(endDateStr);

		List<String> directoryList = new ArrayList<String>();

		int daybet = DateUtil.daysBetween(startDate, endDate);
		for (int i = 0; i < daybet; i++) {
			Date tempDate = DateUtil.addDay(startDate, i);
			String dir = basepath+"/"+DateUtil.dateFormat(tempDate, "yyyyMM") + "/"
					+ DateUtil.dateFormat(tempDate, "yyyyMMdd");
			System.out.println(dir);
			directoryList.add(dir);
		}
		
		return directoryList;
	}
	
	
	
	public static void main(String[] args){
		String startDateStr = "2014-07-11";
		String endDateStr = "2014-07-22";
		genPathByDate(PingbackConstant.pingbackBasePath,startDateStr,endDateStr);
		
	}
}
