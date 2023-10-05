package kokofarm;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.mongodb.MongoException;

import p3300.P3300Listner;
import run_object.RunSql;
import util.DateUtil;
import util.FileUtil;
import util.MongoConn;
import util.MysqlConn;

public class ExternSensor extends Device {
	
	public static ConcurrentHashMap<String, ExternSensor> list_map = new ConcurrentHashMap<String, ExternSensor>(); 
	
	public static ExternSensor get_inst(String m_farm, String m_dong, FarmInfo m_info) {
		
		// 이미 존재하면
		if(list_map.containsKey(m_farm + m_dong)){
			return list_map.get(m_farm + m_dong);
		}
		
		// 없는 경우 생성해서 리턴
		ExternSensor instance = new ExternSensor(m_farm, m_dong, m_info);
		list_map.put(m_farm + m_dong, instance);
		return instance;
	}
	
	private FarmInfo info;
	private boolean need_cache_load = true;
	String sfFeedDate = "";
	int sfFeed = 0, sfDailyFeed = 0, sfAllFeed = 0, sfWater = 0, sfDailyWater = 0, sfAllWater = 0;		// 이전 버퍼 값
	
	private ExternSensor(String m_farm, String m_dong, FarmInfo m_info){
		info = m_info;
		
		farm_id = m_farm;
		dong_id = m_dong;
		doc = new Document();
	}
	
	public void cache_load(){
		String query = "SELECT sfFeedDate, sfFeed, sfDailyFeed, sfAllFeed, sfWater, sfDailyWater, sfAllWater FROM set_feeder WHERE sfFarmid = '" + farm_id + "' AND sfDongid = '" + dong_id + "'";
		List<HashMap<String, Object>> feed_info = MysqlConn.get_sql().select(query, new String[]{"sfFeedDate", "sfFeed" , "sfDailyFeed", "sfAllFeed", "sfWater", "sfDailyWater", "sfAllWater"});
		
		if(feed_info.size() > 0){
			sfFeedDate = 	(String) feed_info.get(0).get("sfFeedDate");
			sfFeed = 		(int) feed_info.get(0).get("sfFeed");
			sfDailyFeed = 	(int) feed_info.get(0).get("sfDailyFeed");
			sfAllFeed = 	(int) feed_info.get(0).get("sfAllFeed");
			sfWater = 		(int) feed_info.get(0).get("sfWater");
			sfDailyWater = 	(int) feed_info.get(0).get("sfDailyWater");
			sfAllWater = 	(int) feed_info.get(0).get("sfAllWater");
			
			need_cache_load = false;
		}
		else{
			FileUtil.write("ERROR => has no feed data / in ext_sensor " + farm_id + " " + dong_id + " / " + query);
		}
	}
	
	public void set_need_cache_load(boolean need_cache_load){
		this.need_cache_load = need_cache_load;
	}
	
	public void update(boolean has_ext){
		
		// 초기화 안된 경우 db에서 가져옴
		if(need_cache_load){
			cache_load();		// 현재 버퍼 테이블값을 읽어옴
		}
		
		int feed_weight = (int) get_val("feedWeight");
		int feed_water = (int) get_val("feedWater");
		
		int feed_val = 0;
		int water_val = 0;
		
		// 사료빈 센서 단선 단락 오류 처리
		if(feed_weight >= 0) {
			feed_val = sfFeed - feed_weight; // 이전 사료빈 무게 - 현재 사료빈 무게 (사료 섭취량)
			feed_val = feed_val < -100 ? 0 : feed_val;		// 현재 사료빈 무게가 이전 값보다 100kg 이상 측정된 경우 사료가 충전되었다고 판단하여 0으로 처리
		}
		else {
			feed_weight = sfFeed;		// 오류인 경우에는 최근값을 유지하여 넣음
		}
		
		// 음수량 센서 단선 단락 오류 처리
		if(feed_water >= 0) {
			water_val = feed_water - sfWater; // 현재 유량센서 값 - 이전 유량센서 값 (음수량)
			water_val = sfFeedDate == null || sfFeedDate.isEmpty() ? 0 : water_val;		// 첫 데이터인 경우 오류 처리
			water_val = water_val < -100 ? water_val + 60000 : water_val;	// 유량센서 값이 max를 넘어서 초기화 된경우 max값인 60000을 더해서 계산
		}
		else {
			feed_water = sfWater;		// 오류인 경우에는 최근값을 유지하여 넣음
		}
		
		// 섭취량 저장
		doc.append("feedWeightval", feed_val);
		doc.append("feedWaterval", water_val);
		
		try {
			MongoConn.get_mongo().get_db().getCollection("sensorExtData").insertOne(doc);
		} catch (MongoException e) {
			FileUtil.write("ERROR => MongoException in ExternSensor : " + e.getMessage());
		}
		
		buffer_update(has_ext, feed_weight, feed_water, feed_val, water_val);
	}
	
	private void buffer_update(boolean has_ext, int feed_weight, int feed_water, int feed_val, int water_val) {
		
		String get_time = (String) get_val("getTime");
		
		//rdb 업데이트
		String s_table = "set_feeder AS sf";
		String s_where = "sf.sfFarmid = '" + farm_id + "' AND sf.sfDongid = '" + dong_id + "'";
		HashMap<String, String> update_map = new HashMap<String, String>();
		
		// 수집시간 및 현재 상태는 그대로 업데이트
		update_map.put("sfFeedDate", get_time);				
		update_map.put("sfFeed", Integer.toString(feed_weight));		
		update_map.put("sfWaterDate", get_time);						
		update_map.put("sfWater", Integer.toString(feed_water));
		
		int prev_interm = DateUtil.get_inst().get_total_days("2000-01-01 00:00:00", sfFeedDate == null || sfFeedDate.isEmpty() ? DateUtil.get_inst().get_now() : sfFeedDate);		// 가장 마지막 수집시간의 일자
		int curr_interm = DateUtil.get_inst().get_total_days("2000-01-01 00:00:00", get_time);		// 현재 수집시간의 일자
		
		// 일령 변경 확인
		if(curr_interm > prev_interm){		// 가장 마지막 수집한 시간과 현재 시간의 일자가 다른경우 - 날짜가 바뀐경우
			update_map.put("sfPrevFeed", Integer.toString(sfDailyFeed));		// 전일 급이량에 현재 일일 급이량 값을 넣음
			update_map.put("sfDailyFeed", Integer.toString(feed_val));
			sfDailyFeed = feed_val;
			
			update_map.put("sfPrevWater", Integer.toString(sfDailyWater));		// 전일 급수량에 현재 일일 급수량 값을 넣음
			update_map.put("sfDailyWater", Integer.toString(water_val));
			sfDailyWater = water_val;
		}
		else{	// 같은 날짜 인경우
			sfDailyFeed += feed_val;
			sfDailyWater += water_val;
			
			update_map.put("sfDailyFeed", Integer.toString(sfDailyFeed));
			update_map.put("sfDailyWater", Integer.toString(sfDailyWater));
		}
		
		sfAllFeed += feed_val;
		sfAllWater += water_val;
		update_map.put("sfAllFeed", Integer.toString(sfAllFeed));	// 전체 급이량
		update_map.put("sfAllWater", Integer.toString(sfAllWater));	// 전체 급수량
		
		// 오류 상태 설정
		update_map.put("sfFeedError", check_err_status((int) get_val("feedWeight")));	// 전체 급이량
		update_map.put("sfWaterError", check_err_status((int) get_val("feedWater")));	// 전체 급수량
		
		// rdb 적재 후 캐시 데이터 최신화
		sfFeedDate = get_time;
		sfFeed = feed_weight;
		sfWater = feed_water;
		
		double temp = (double) get_val("outTemp");
		double humi = (double) get_val("outHumi");
		double nh3 = (double) get_val("outNh3");
		double h2s = (double) get_val("outH2s");
		double w_speed = (double) get_val("outWinspeed");
		
		int pm10 = (int) get_val("outDust");
		int pm25 = (int) get_val("outUDust");
		int w_direction = (int) get_val("outWinderec");
		int solar = (int) get_val("outSolar");
		
		if(has_ext){
			s_table += " JOIN set_outsensor AS so ON so.soFarmid = sf.sfFarmid AND so.soDongid = sf.sfDongid";
			update_map.put("soSensorDate", get_time);
			update_map.put("soTemp", String.format("%.1f", temp));
			update_map.put("soHumi", String.format("%.1f", humi));
			update_map.put("soNh3", String.format("%.1f", nh3));
			update_map.put("soH2s", String.format("%.1f", h2s));
			update_map.put("soDust", Integer.toString(pm10));
			update_map.put("soUDust", Integer.toString(pm25));
			update_map.put("soWindDirection", Integer.toString(w_direction));
			update_map.put("soWindSpeed", String.format("%.1f", w_speed));
			update_map.put("soSolar", Integer.toString(solar));
			update_map.put("soError", check_err_status((int) temp));
		}
		
		//P3300Listner.update_executors.execute(new RunSql(s_table, s_where, update_map, "COMPLETE => Extra Data Insert Completed " + farm_id + " " + dong_id));
		P3300Listner.update_executors.execute(new RunSql(s_table, s_where, update_map, ""));
		
		// 히스토리 저장
		info.put_history("feed_feed", feed_val);			// 급이량
		info.put_history("feed_water", water_val);			// 급수량
		
		info.put_history("ext_temp", temp);					// 외기온도
		info.put_history("ext_humi", humi);					// 외기습도
		info.put_history("ext_nh3", nh3);					// 외기암모니아
		info.put_history("ext_h2s", h2s);					// 외기황화수소
		info.put_history("ext_dust", pm10);					// 미세먼지
		info.put_history("ext_udust", pm25);				// 초미세먼지
		info.put_history("ext_wspeed", w_speed);			// 풍속
		info.put_history("ext_wdirec", w_direction);		// 풍향
		info.put_history("ext_solar", solar);				// 일사량
	}
	
	private String check_err_status(int val) {
		
		String ret = "N0";
		
		switch (val) {
		case -100:
			ret = "E1";
			break;

		case -200:
			ret = "E2";
			break;
		}
		
		return ret;
	}
}
