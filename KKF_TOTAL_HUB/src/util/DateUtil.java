package util;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

//********************************************************
//class		my_date
//role		날짜 관련 함수 모음, LocalDateTime 관련 기능 모음
//********************************************************
public class DateUtil {
	private static DateUtil m = new DateUtil();	//싱글턴 인스턴스
	private final LocalDateTime base_time = LocalDateTime.parse("1970/01/01 00:00:00", DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"));		//timestamp를 위한 기준시간
	private final DateTimeFormatter default_form = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private final DateTimeFormatter date_form = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private final DateTimeFormatter simple_form = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	private final DateTimeFormatter log_form = DateTimeFormatter.ofPattern("yyyyMMdd");
	
	public static void main(String[] args) {
		long tt = DateUtil.get_inst().get_timestamp("2022-01-01 00:00:00");
		
		System.out.println(tt);
		
		System.out.println(1650641780 - tt);
	}
	
	private DateUtil(){
		//System.out.println("[" + m.get_now() + "] CREATE => Date Util Created");
	}
	
	public static DateUtil get_inst() {
		return m;
	}
	
	//**********************************************
	// 현재시간을 형식없이 가져옴
	// return : ex) 20200110155530 
	//**********************************************
	public String get_now_simple() {
		return LocalDateTime.now().format(simple_form);		//"yyyyMMddHHmmss"
	}
	
	//**********************************************
	// 현재시간을 일자까지만 가져옴
	// return : ex) 20200110
	//**********************************************
	public String get_now_for_log() {
		return LocalDateTime.now().format(log_form);
	}
	
	//**********************************************
	// 현재시간을 기본 형식으로 가져옴
	// return : ex) 2020-01-10 15:55:30
	//**********************************************
	public String get_now() {
		return LocalDateTime.now().format(default_form);	//"yyyy-MM-dd HH:mm:ss"
	}
	
	//**********************************************
	// base_time과 파라미터로 들어온 시간의 차이를 초로 환산하여 리턴
	//**********************************************
	public String get_timestamp(String year, String month, String date, String hour, String min, String sec) {
		String ret = "";
		
		LocalDateTime now_date = LocalDateTime.parse("20" + year + month + date + hour + min + sec, simple_form);	//"yyyyMMddHHmmss"
		Duration duration = Duration.between(base_time, now_date);
		
		ret = Long.toString(duration.getSeconds());
		
		return ret;
	}
	public long get_timestamp(String time) {
		long ret = 0;
		
		try {
			LocalDateTime now_date = LocalDateTime.parse(time, default_form);	//"yyyy-MM-dd HH:mm:ss"
			Duration duration = Duration.between(base_time, now_date);
			ret = duration.getSeconds();
			
		}catch(NullPointerException e) {
			e.printStackTrace();
			FileUtil.write("ERROR => Get Timestamp Error / Class : my_date");
		}catch(Exception e) {
			e.printStackTrace();
			FileUtil.write("ERROR => Get Timestamp Error / Class : my_date");
		}
		
		return ret;
	}
	
	//**********************************************
	// base_time과 현재 시간의 차이를 초로 환산하여 리턴
	//**********************************************
	public long get_now_timestamp() {
		Duration duration = Duration.between(base_time, LocalDateTime.now());
		return duration.getSeconds();
	}
	public int get_now_nano() {
		Duration duration = Duration.between(base_time, LocalDateTime.now());
		return duration.getNano();
	}
	
	public String timestamp_to_date(long stamp){
        return base_time.plusSeconds(stamp).format(default_form);
    }
	
	//**********************************************
	// 분을 더하거나 뺀 시간을 리턴
	//**********************************************
	public String get_plus_minus_minute_time(String parse_time, int gap) {
		
		if(gap > 0) {
			LocalDateTime tp = LocalDateTime.parse(parse_time, default_form);	//"yyyy-MM-dd HH:mm:ss"
			return tp.plusMinutes(gap).format(default_form);
		}else {
			gap = gap * -1;
			LocalDateTime tp = LocalDateTime.parse(parse_time, default_form);
			return tp.minusMinutes(gap).format(default_form);
		}
	}
	
	//**********************************************
	// 일령 구하기
	//**********************************************
	public int get_days(String start) {
		return get_days(start, get_now());
	}
	public int get_days(String start, String end) {
		//날짜 일령
		//return get_total_days(start, end);
		
		//시차 일령
		LocalDateTime start_time = LocalDateTime.parse(start, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		LocalDateTime end_time = LocalDateTime.parse(end, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		Duration duration = Duration.between(start_time, end_time);
		
		int ret = (int) (duration.getSeconds() / 86400) + 1;
		
		//file_io.write("TEST => S : " + start + " E : " + end + " days : " + ret );
		
		return ret;
	}
	public int get_total_days(String start, String end) {
		try {
			LocalDate start_date = LocalDate.parse(start.substring(0, 10), date_form);		//"yyyy-MM-dd"
			LocalDate end_date = LocalDate.parse(end.substring(0, 10), date_form);
			
			return (int) ChronoUnit.DAYS.between(start_date, end_date) + 1;
		}catch(StringIndexOutOfBoundsException e) {
			FileUtil.write("ERROR => Out of Index / Class : my_date / Func : get_total_days");
			return -1;
		}catch(Exception e) {
			FileUtil.write("ERROR => Exception / Class : my_date / Func : get_total_days");
			return -1;
		}
	}
	
	//시간의 차이를 초로 환산
	public int get_duration(String start, String end){
		LocalDateTime start_time = LocalDateTime.parse(start, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		LocalDateTime end_time = LocalDateTime.parse(end, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
		Duration duration = Duration.between(start_time, end_time);
		
		//int ret = (int) (duration.getSeconds() / 86400) + 1;
		
		//file_io.write("TEST => S : " + start + " E : " + end + " days : " + ret );
		
		return (int) duration.getSeconds();
	}
	
	//**********************************************
	// 자정 시간 구하기
	//**********************************************
	public String get_midNight(String parse_time) {
		String ret = "";
		LocalDateTime tp = LocalDateTime.parse(parse_time, default_form);	//"yyyy-MM-dd HH:mm:ss"
		ret = tp.plusDays(1).format(default_form).substring(0, 10) + " 00:00:00";
		return ret;
	}
	
	public String get_now_millisecond(){
		LocalDateTime now = LocalDateTime.now();
		Duration duration = Duration.between(base_time, now);
		
		String ret = now.format(default_form);
		ret += "/" + Integer.toString(duration.getNano());
		return ret;
	}
}
