package sogou.pingback.util;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;


/**
 * ���ڹ���
 * 
 * @author wm
 * 
 */
public class DateUtil {

	public static final SimpleDateFormat date = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static final SimpleDateFormat year = new SimpleDateFormat("yyyy");
	public static final SimpleDateFormat month = new SimpleDateFormat("MM");
	public static final SimpleDateFormat day = new SimpleDateFormat("dd");
	public static final SimpleDateFormat yeatMonth = new SimpleDateFormat(
			"yyyyMM");
	public static final SimpleDateFormat datetime = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat worddate = new SimpleDateFormat(
			"yyyyMMddHHmmss");

	public static final String dateformatstr1 = "yyyy-MM-dd HH:mm:ss";
	public static Date currentDate() {
		GregorianCalendar calenda = new GregorianCalendar();
		return calenda.getTime();
	}

	public static Date currentDay() {
		try {
			return date.parse(date.format(new Date()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
		}
		return null;
	}

	private static void setMinOfDay(Calendar c) {
		c.set(Calendar.HOUR, c.getActualMinimum(Calendar.HOUR));
		c.set(Calendar.MINUTE, c.getActualMinimum(Calendar.MINUTE));
		c.set(Calendar.SECOND, c.getActualMinimum(Calendar.SECOND));
		c.set(Calendar.MILLISECOND, c.getActualMinimum(Calendar.MILLISECOND));
		c.set(Calendar.AM_PM, Calendar.AM);
	}

	private static void setMaxOfDay(Calendar c) {
		c.set(Calendar.HOUR, c.getActualMaximum(Calendar.HOUR));
		c.set(Calendar.MINUTE, c.getActualMaximum(Calendar.MINUTE));
		c.set(Calendar.SECOND, c.getActualMaximum(Calendar.SECOND));
		c.set(Calendar.MILLISECOND, c.getActualMaximum(Calendar.MILLISECOND));
		c.set(Calendar.AM_PM, Calendar.PM);
	}

	// ������ܵ�һ��
	public static Date getPreviousWeekMonday() {

		int mondayPlus = getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus - 7);
		setMinOfDay(currentDate);
		Date monday = currentDate.getTime();
		return monday;
	}

	// ������������յ�����
	public static Date getPreviousWeekSunday() {

		int mondayPlus = getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus - 1);
		setMaxOfDay(currentDate);
		Date sunday = currentDate.getTime();
		return sunday;
	}

	// ��ñ��ܵ�һ��
	public static Date getCurrentWeekMonday() {

		int mondayPlus = getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus);
		setMinOfDay(currentDate);
		Date monday = currentDate.getTime();
		return monday;
	}

	// ��ñ������һ��
	public static Date getCurrentWeekSunday() {

		int mondayPlus = getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 6);
		setMaxOfDay(currentDate);
		Date sunday = currentDate.getTime();
		return sunday;
	}
	
	/**
	 * ��ȡĳ��ʱ�����һ
	 * @param date
	 * @return
	 */
	public static Date getChineseWeekMonday(Date date){
		Calendar c = new GregorianCalendar();
	      c.setFirstDayOfWeek(Calendar.MONDAY);
	      c.setTime(date);
	      c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
//	      System.out.println("c"+c.getTime());
	      return c.getTime();
	}
	
	public static Date getChineseWeekMondayCurrent( ){
		Calendar c = new GregorianCalendar();
	      c.setFirstDayOfWeek(Calendar.MONDAY);
	      c.setTime(DateUtil.currentDate());
	      c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
//	      System.out.println("c"+c.getTime());
	      return c.getTime();
	}

	// ���µ�һ��
	public static Date getPreviousMonthFirst() {

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// ��Ϊ��ǰ�µ�1��
		lastDate.add(Calendar.MONTH, -1);// ��һ���£���Ϊ���µ�1��
		setMinOfDay(lastDate);
		return lastDate.getTime();
	}

	// ����������һ�������
	public static Date getPreviousMonthLast() {

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.MONTH, -1);// ��һ����
		lastDate.set(Calendar.DATE, 1);// ����������Ϊ���µ�һ��
		lastDate.roll(Calendar.DATE, -1);// ���ڻع�һ�죬Ҳ���Ǳ������һ��
		setMaxOfDay(lastDate);
		return lastDate.getTime();
	}

	// ��ȡ���µ�һ��
	public static Date getCurrentMonthFirst() {

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// ��Ϊ��ǰ�µ�1��
		setMinOfDay(lastDate);
		return lastDate.getTime();
	}

	// ���㵱�����һ��
	public static Date getCurrentMonthLast() {

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// ��Ϊ��ǰ�µ�1��
		lastDate.add(Calendar.MONTH, 1);// ��һ���£���Ϊ���µ�1��
		lastDate.add(Calendar.DATE, -1);// ��ȥһ�죬��Ϊ�������һ��
		setMaxOfDay(lastDate);
		return lastDate.getTime();
	}

	// ���㵱�����һ��
	public static String getCurrentStrMonthLast() {

		String pattern = "yyyy-MM-dd";
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		String tString = df.format(getCurrentMonthLast());
		return tString;
	}

	// ��õ�ǰ�����뱾������������
	private static int getMondayPlus() {
		Calendar cd = Calendar.getInstance();
		// ��ý�����һ�ܵĵڼ��죬�������ǵ�һ�죬���ڶ��ǵڶ���......
		int dayOfWeek = cd.get(Calendar.DAY_OF_WEEK) - 1; // ��Ϊ���й����һ��Ϊ��һ�����������1
		if (dayOfWeek == 1) {
			return 0;
		} else {
			return 1 - dayOfWeek;
		}
	}

	public static Date getSchedulerStart(String value) {
		Date start = getSchedulerStartConfiguration(value);
		if (start.before(new Date())) {
			start = addDay(start, 1);
		}
		return start;
	}

	private static Date getSchedulerStartConfiguration(String value) {
		GregorianCalendar calenda = new GregorianCalendar();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return parseDate(sdf.format(calenda.getTime()) + " " + value);
	}

	public static String dateFormat(Date myDate,String fromatString){

	     SimpleDateFormat myFormat = new SimpleDateFormat(fromatString); 
	     return myFormat.format(myDate);

	}
	
	public static Date parseDate(String dateStr) {
		if (dateStr == null)
			return null;
		String[] patterns = {"yyyy-MM-dd HH:mm:ss" , "yyyy-MM-dd HH:mm",
				"yyyy:MM:dd HH:mm:ss", "yyyy-MM-dd", "dd.MM.yy HH:mm",
				"yyyyMMdd HHmmss", "yyyyMMdd HHmm", "MM/dd/yy hh:mm a",
				"HH:mm:ss dd.MM.yyyy", "yyyy:MM:dd", "yyyy:MM:dd HH:mm",
				"dd.MM.yy", "yyyyMMdd", "EEE, dd MMM yyyy HH:mm:ss",
				"MM/dd/yy", "yyyy:MM:dd HH:mm:sss", "yyyy/MM/dd", "yyyy-MM-dd" };
		for (int j = 0; j < patterns.length; j++) {
			try {
				DateFormat parser = new SimpleDateFormat(patterns[j],
						Locale.ENGLISH);
				return parser.parse(dateStr);
			} catch (ParseException e) {
			}
		}
		return null;
	}


	public static List<String> dateRegion(String fromDate, String endDate) {
		try {
			List<String> dateList = new ArrayList<String>();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date begin = sdf.parse(fromDate);
			Date end = sdf.parse(endDate);
			double between = (end.getTime() - begin.getTime()) / 1000;// ����1000��Ϊ��ת������
			double day = between / (24 * 3600);
			for (int i = 0; i <= day; i++) {
				Calendar cd = Calendar.getInstance();
				cd.setTime(sdf.parse(fromDate));
				cd.add(Calendar.DATE, i);// ����һ��
				dateList.add(sdf.format(cd.getTime()));
			}
			return dateList;
		} catch (Exception e) {
			return null;
		}
	}

	public static Date addDay(Date inDate, int day) {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(inDate);
		calendar.add(Calendar.DATE, day);
		return calendar.getTime();
	}
	
	/**
	 * 
	 * @param startDate
	 * @param in
	 * @return
	 */
//	public static Date endDateIn(Date startDate, int in){
//
//		   SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd"); 
//		   df.format(new Date(startDate - in * 24 * 60 * 60 * 1000));
//		return calendar.getTime();
//	}

	public static String addDay2(String inDate, int day) {
		Date datetemp = parseDate(inDate);
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(datetemp);
		calendar.add(Calendar.DATE, day);
		return date.format(calendar.getTime());
	}

	/**
	 * ��ȡĳ��ʱ��ε�day
	 * 
	 * @param date
	 * @return
	 */
	public static Date getDateDayDate(Date date) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		String tString = df.format(date);
		
		return parseDate(tString);
		
//		return tString;
	}
	
	
	/**
	 * ��ȡĳ��ʱ��ε�day
	 * 
	 * @param date
	 * @return
	 */
	public static String getDateDay(Date date) {
		String pattern = "yyyy-MM-dd";
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		String tString = df.format(date);
		return tString;
	}
	public static String getDateHour(Date date) {
		String pattern = "HH";
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		String tString = df.format(date);
		return tString;
	}
	public static String getDateMinute(Date date) {
		String pattern = "mm";
		SimpleDateFormat df = new SimpleDateFormat(pattern);
		String tString = df.format(date);
		return tString;
	}
	
	
	public static final int daysBetween(Date early, Date late) { 
	     
        java.util.Calendar calst = java.util.Calendar.getInstance();   
        java.util.Calendar caled = java.util.Calendar.getInstance();   
        calst.setTime(early);   
         caled.setTime(late);   
         //����ʱ��Ϊ0ʱ   
         calst.set(java.util.Calendar.HOUR_OF_DAY, 0);   
         calst.set(java.util.Calendar.MINUTE, 0);   
         calst.set(java.util.Calendar.SECOND, 0);   
         caled.set(java.util.Calendar.HOUR_OF_DAY, 0);   
         caled.set(java.util.Calendar.MINUTE, 0);   
         caled.set(java.util.Calendar.SECOND, 0);   
        //�õ�����������������   
         int days = ((int) (caled.getTime().getTime() / 1000) - (int) (calst   
                .getTime().getTime() / 1000)) / 3600 / 24;   
         
        return days;   
   }   
  
   
   public static void test()
   {
        Date earlydate = new Date();   
        Date latedate = new Date();   
        DateFormat df = DateFormat.getDateInstance();   
        try {   
            earlydate = df.parse("2009-10-21");   
            latedate = df.parse("2009-10-10");   
        } catch (ParseException e) {   
              e.printStackTrace();   
          }   
         int days = daysBetween(earlydate,latedate);   
         System.out.println(days);   
   }
   
   
   
   /**
    * ��ȡ��ǰ���������ڼ�<br>
    * 
    * @param dt
    * @return ��ǰ���������ڼ�
    */
   public static String getWeekOfDate(Date dt) {
       String[] weekDays = {"������", "����һ", "���ڶ�", "������", "������", "������", "������"};
       Calendar cal = Calendar.getInstance();
       cal.setTime(dt);
       int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
       if (w < 0)
           w = 0;
       return weekDays[w];
   }
   
   
   /**
    * ��ȡ��ǰ���������ڼ�<br>
    * 
    * @param dt
    * @return ��ǰ���������ڼ�
    */

   public static String getWeekOfDateEn(Date dt) {
       String[] weekDays = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
       Calendar cal = Calendar.getInstance();
       cal.setTime(dt);
       int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
       if (w < 0)
           w = 0;
       return weekDays[w];
   }
   
   
   public static String getWeekOfDateAbbrEn(Date dt) {
       String[] weekDays = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
       Calendar cal = Calendar.getInstance();
       cal.setTime(dt);
       int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
       if (w < 0)
           w = 0;
       return weekDays[w];
   }
   
   
   public static String getChineseTime(Date date) {
		try {
			String a = "";
			a = year.format(date)+"��"+month.format(date)+"��"+day.format(date)+"��";
			return a;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}
		return null;
	}
   
    public static String getEnglishTime(Date date) {
		try {
			String a = "";
			DateFormat df1 = new SimpleDateFormat("MMM-dd'th', yyyy",Locale.ENGLISH); 
			a = df1.format(date);
			return a;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}
		return null;
	}
    

	
	public static void main(String[] args) {
		System.out.println(DateUtil.currentDate());
		System.out.println(DateUtil.getDateDay(DateUtil.currentDate()));
		System.out.println(DateUtil.getDateMinute(DateUtil.currentDate()));
		
		
		System.out.println(DateUtil.daysBetween(DateUtil.currentDate(), DateUtil.addDay(DateUtil.currentDate(), 2)));
		test();
	}
}
