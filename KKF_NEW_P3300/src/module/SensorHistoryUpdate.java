package module;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kokofarm.FarmInfo;
import util.FileUtil;
import util.HashBuffer;
import util.MysqlConn;
import util.DateUtil;

public class SensorHistoryUpdate {
	private static SensorHistoryUpdate inst = null;
	
	public static SensorHistoryUpdate get_inst(){
		if(inst == null){
			inst = new SensorHistoryUpdate();
		}
		
		return inst;
	}
	
	ScheduledExecutorService fix_second_service;
	ScheduledExecutorService fix_minute_service;
	ScheduledExecutorService update_service;
	
	final String[] cell_keys = new String[]{"cell_temp", "cell_humi", "cell_co2", "cell_nh3", "cell_dust"};
	final String[] feed_keys = new String[]{"feed_feed", "feed_water"};
	final String[] extern_keys = new String[]{"ext_temp", "ext_humi", "ext_nh3", "ext_h2s", "ext_dust", "ext_udust", "ext_wspeed", "ext_wdirec", "ext_solar"};
	final String[] light_keys = new String[]{"light_01", "light_02", "light_03", "light_04"};
	
	private SensorHistoryUpdate(){
		fix_second();
	}
	
	private void fix_second(){
		//**************************************************************
    	// 초 단위 교정
    	//**************************************************************
        Runnable fix_second_timer = new Runnable() {
            @Override
            public void run() {
            	String now_second = DateUtil.get_inst().get_now_simple().substring(12);
    			if(now_second.equals("10")) {		//현재 초가 10초면 평균중량 산출 타이머를 실행
    				//start();
    				fix_minute();
    				fix_second_service.shutdownNow();
    			}
            }
        };
        fix_second_service = Executors.newSingleThreadScheduledExecutor();
        fix_second_service.scheduleAtFixedRate(fix_second_timer, 0, 1000, TimeUnit.MILLISECONDS);
	}
	
	private void fix_minute(){
		//**************************************************************
    	// 분 단위 교정
    	//**************************************************************
        Runnable fix_minute_timer = new Runnable() {
            @Override
            public void run() {  
            	String now_minute = DateUtil.get_inst().get_now_simple().substring(10, 12);
    			if(now_minute.equals("00")) {		//각 시간 00분 10초에 실행
    				//start();
    				fix_minute_service.shutdownNow();
    			}
            }
        };
        fix_minute_service = Executors.newSingleThreadScheduledExecutor();
        fix_minute_service.scheduleAtFixedRate(fix_minute_timer, 0, 60000, TimeUnit.MILLISECONDS);
	}
	
	private void start(){
		Runnable update_timer = new Runnable() {
            @Override
            public void run() {
            	
            	try {
					String test_start = DateUtil.get_inst().get_now();
					
					String time = DateUtil.get_inst().get_now().substring(0, 17) + "00";
					//String time = DateUtil.get_inst().get_now();
					
					String insert_query = "INSERT INTO sensor_history(shFarmid, shDongid, shDate, shSensorData, shFeedData, shExtSensorData, shLightData) VALUES ";
					String values_query = "";
					String obj_query = "";
					
					// 전체 농장 데이터 순회
					for(Entry<String, FarmInfo> entry : FarmInfo.list_map.entrySet()){
						FarmInfo info = entry.getValue();
						HashBuffer buffer = info.get_history_buffer();
						
						values_query += "('" + info.get_farm_id() + "', '" + info.get_dong_id() + "', '" + time + "', JSON_OBJECT(";
						
						String value = "";
						// 저울 데이터 순회
						obj_query = "";
						for(String key : cell_keys){
							if(buffer.containsKey(key)){
								value = String.format("%.2f", buffer.get_avg_item(key));
								obj_query += "'" + key + "', " + value + ", ";
							}
						}
						obj_query = obj_query.isEmpty() ? "" : obj_query.substring(0, obj_query.length() - 2);
						values_query += obj_query + ")";
						
						// 급이 급수 및 외기환경 데이터 순회
						if(info.has_extern_sensor()){
							
							values_query += ", JSON_OBJECT(";
							obj_query = "";
							for(String key : feed_keys){
								if(buffer.containsKey(key)){
									value = String.format("%.0f", buffer.get_sum_item(key));		// 급이 급수는 합계를 구함
									obj_query += "'" + key + "', " + value + ", ";
								}
							}
							obj_query = obj_query.isEmpty() ? "" : obj_query.substring(0, obj_query.length() - 2);
							values_query += obj_query + ")";
							
							// 외기환경 존재시 
							values_query += ", JSON_OBJECT(";
							obj_query = "";
							for(String key : extern_keys){
								if(buffer.containsKey(key)){
									value = String.format("%.2f", buffer.get_avg_item(key));
									obj_query += "'" + key + "', " + value + ", ";
								}
							}
							obj_query = obj_query.isEmpty() ? "" : obj_query.substring(0, obj_query.length() - 2);
							values_query += obj_query + ")";
						}
						else{
							values_query += ", JSON_OBJECT(), JSON_OBJECT()";
						}
						
						// 조도센서 데이터 순회
						if(info.has_light_sensor()) {
							
							values_query += ", JSON_OBJECT(";
							obj_query = "";
							for(String key : light_keys){
								if(buffer.containsKey(key)){
									value = String.format("%.0f", buffer.get_avg_item(key));
									obj_query += "'" + key + "', " + value + ", ";
								}
							}
							obj_query = obj_query.isEmpty() ? "" : obj_query.substring(0, obj_query.length() - 2);
							values_query += obj_query + ")";
						}
						else{
							values_query += ", JSON_OBJECT()";
						}
						
						values_query += "), ";
					}
					
					if(!values_query.isEmpty()){
						values_query = values_query.substring(0, values_query.length() - 2);
						
						insert_query += values_query;
						
						Statement state = MysqlConn.get_sql().get_statement();
						try {
							if(state != null) {
								state.executeUpdate(insert_query);		//업데이트 수행
								state.close();
							}
							
							FileUtil.write("COMPLETE => Sensor History Update / sensor_history_update");
							
						} catch (SQLException e) {
							e.printStackTrace();
							FileUtil.write("ERROR => Sensor History Update Error / sensor_history_update / " + insert_query);
						} catch (Exception e) {
							e.printStackTrace();
							FileUtil.write("ERROR => Sensor History Update Error / sensor_history_update / " + insert_query);
						}
					}
					
					String test_end = DateUtil.get_inst().get_now();
					
					int term = DateUtil.get_inst().get_duration(test_start, test_end);
					FileUtil.write("TEST => sensor_history end : " + test_end + " term : " + term + insert_query);
				} catch (Exception e) {
					FileUtil.write("ERROR => Sensor History Update Error " + e.getMessage());
				}
            	
            }
        };
        update_service = Executors.newSingleThreadScheduledExecutor();
        update_service.scheduleAtFixedRate(update_timer, 0, 60000 * 60, TimeUnit.MILLISECONDS);
        //update_service.scheduleAtFixedRate(update_timer, 0, 60000, TimeUnit.MILLISECONDS);
	}
}
