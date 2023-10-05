package module;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import kokofarm.ExternSensor;
import kokofarm.FarmInfo;
import p3300.P3300Listner;
import run_object.RunCamera;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.MysqlConn;

public class CheckComein {
	private static CheckComein inst = null;
	
	public static CheckComein get_inst(){
		if(inst == null){
			inst = new CheckComein();
		}
		
		return inst;
	}
	
	private CheckComein(){}
	
	// 출하 상태에서 -> 입추 상태로 변경 (입추 처리 로직)
	private void OtoI(String farmID, String dongID, String comein_time, String type, String feeder){
		String max_ref = "6";
		String avg_ratio_sql = "SELECT cmFarmid, cmDongid, IF(cmRatio = 0.0, 0.7, cmRatio) AS ratio FROM comein_master "
				+ "WHERE cmFarmid = '" + farmID + "' AND cmDongid = '" + dongID + "' AND cmIntype = '" + type + "' ORDER BY cmIndate DESC LIMIT " + max_ref + ";";
		
		// ratio 기록을 가져와서 평균을 냄 
		double double_avg_ratio = 0.0;
		int ref_cnt = 0;
		
		Statement state = MysqlConn.get_sql().get_statement();
		ResultSet set = null;
		if(state != null) {
			try {
				set = state.executeQuery(avg_ratio_sql);
				
				while(set.next()) {
					ref_cnt++;
					double_avg_ratio = FloatCompute.plus(double_avg_ratio, set.getDouble("ratio"));
				}
				
			} catch (SQLException e) {
				FileUtil.write("ERROR => SQLException Select Error in avg_ratio / Class : check_come_in / query : " + avg_ratio_sql);
			} catch (Exception e) {
				FileUtil.write("ERROR => Exception Select Error in avg_ratio / Class : check_come_in / query : " + avg_ratio_sql);
			}
		}
		
		try {
			if(set != null) {
				set.close();
			}
			state.close();
		} catch (SQLException e) {
			FileUtil.write("ERROR => SQLException Close Error in OtoI / Class : check_come_in ");
		}
		
		if(ref_cnt == 0){
			double_avg_ratio = 0.7;
		}
		else{
			double_avg_ratio += FloatCompute.multiply(0.7, (Integer.parseInt(max_ref) - ref_cnt));
			double_avg_ratio = FloatCompute.divide(double_avg_ratio, Integer.parseInt(max_ref));
		}
		
		String avg_ratio = String.format("%.4f", double_avg_ratio);
		
		//comein_master 데이터 추가
		HashMap<String, String> update_map = new HashMap<String, String>();
		String code = DateUtil.get_inst().get_now_simple() + "_" + farmID + dongID;
		update_map.put("cmCode", code);
		update_map.put("cmFarmid", farmID);
		update_map.put("cmDongid", dongID);
		update_map.put("cmRatio", avg_ratio);
		update_map.put("cmIntype", type);	
		update_map.put("cmIndate", comein_time);
		MysqlConn.get_sql().insert("comein_master", update_map);
		
		//버퍼 테이블 beStatus 변경
		String where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
		update_map.clear();
		update_map.put("beComeinCode", code);
		update_map.put("beStatus", "I");
		update_map.put("beDays", "1");
		update_map.put("beAvgWeight", "0.0");
		update_map.put("beDevi", "0.0");
		update_map.put("beVc", "0.0");
		MysqlConn.get_sql().update("buffer_sensor_status", where, update_map);
		
		FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'O' to 'I'");
		
		// 입추 시점 평체산출 히스토그램 초기화
		AvgCalcBridge.get_inst().send_empty(farmID, dongID, type, comein_time.substring(0, 10) + " 00:00:00", 0);
		
		// 초기화 한번 한 후에 ratio 변경
		String data_path = "/home/kokofarm/KKF_EWAS/DATA/" + farmID + "/" + dongID + "/Config.json";
		FileUtil.json_modify(data_path , 6, "  \"MedianRatio\": " + avg_ratio + ",");
		
		// 다시 초기화
		AvgCalcBridge.get_inst().send_empty(farmID, dongID, type, comein_time.substring(0, 10) + " 00:00:00", 0);
		
		//입추 시점 급이량 데이터 초기화 - feedWeightval을 feedWeight와 동일하게
		if(!feeder.equals("no")){		// 급이기 데이터 존재시에만 동작
			//feeder_init(in_time);
			
			update_map.clear();
			update_map.put("sfDailyFeed", "0.0");
			update_map.put("sfPrevFeed", "0.0");
			update_map.put("sfAllFeed", "0.0");
			update_map.put("sfDailyWater", "0.0");
			update_map.put("sfPrevWater", "0.0");
			update_map.put("sfAllWater", "0.0");
			MysqlConn.get_sql().update("set_feeder", "sfFarmid = '" + farmID + "' AND sfDongid = '" + dongID + "'", update_map);
			
			ExternSensor.list_map.get(farmID + dongID).set_need_cache_load(true);			// 초기화된 상태를 불러오도록 설정
		}
		
		//입추 시점 데이터 재산출
		CheckRecalc.get_inst().start_recalculate(farmID, dongID, comein_time, DateUtil.get_inst().get_now());
	}
	
	private void EtoI(String farmID, String dongID, String last_avg_date){
		HashMap<String, String> update_map = new HashMap<String, String>();
		update_map.put("beStatus", "I");
		
		String where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
		MysqlConn.get_sql().update("buffer_sensor_status", where, update_map);
		
		FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'E' to 'I'");
		
		if(!FarmInfo.list_map.get(farmID + dongID).get_recalc_status()){
			CheckRecalc.get_inst().start_recalculate(farmID, dongID, DateUtil.get_inst().get_plus_minus_minute_time(last_avg_date, 10), DateUtil.get_inst().get_now());
		}
	}
	
	private void WtoI(String farmID, String dongID, String last_avg_date){
		HashMap<String, String> update_map = new HashMap<String, String>();
		update_map.put("beStatus", "I");
		
		String where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
		MysqlConn.get_sql().update("buffer_sensor_status", where, update_map);
		
		// 솎기 처리 로직
		String camera_sql = "";
		camera_sql += "SELECT scUrl, scPort, be.beIPaddr, be.beAvgWeightDate FROM set_camera";
		camera_sql += " JOIN buffer_sensor_status AS be ON scFarmid = be.beFarmid AND scDongid = be.beDongid";
		camera_sql += " WHERE scFarmid = '" + farmID + "' AND scDongid = '" + dongID + "';";
		
		String prev_time = "";
		
		Statement state = MysqlConn.get_sql().get_statement();
		ResultSet set = null;
		if(state != null) {
			try {
				set = state.executeQuery(camera_sql);
				
				if(set.next()) {
					String s_ip = set.getString("beIPaddr");
					String s_port = set.getString("scPort");
					String s_url = set.getString("scUrl");
					prev_time = set.getString("beAvgWeightDate");
					
					P3300Listner.camera_executors.execute(new RunCamera(s_ip, s_port, s_url, farmID + "/" + dongID, "T"));
				}
			} catch (SQLException e) {
				FileUtil.write("ERROR => SQLException in buffer_update / set_comein / w logic");
			}
		}
		
		try {
			if(set != null) {
				set.close();
			}
			state.close();
		} catch (SQLException e) {
			FileUtil.write("ERROR => SQLException Close Error in WtoI / Class : check_come_in ");
		}
		
		update_map.clear();
		update_map.put("ccFarmid", farmID);
		update_map.put("ccDongid", dongID);
		update_map.put("ccCapDate", DateUtil.get_inst().get_now());
		update_map.put("ccStatus", "T");
		update_map.put("ccPrvDate", prev_time);
		MysqlConn.get_sql().insert("capture_camera", update_map);
		
		FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'W' to 'I'");
		
		if(!FarmInfo.list_map.get(farmID + dongID).get_recalc_status()){
			CheckRecalc.get_inst().start_recalculate(farmID, dongID, DateUtil.get_inst().get_plus_minus_minute_time(last_avg_date, 10), DateUtil.get_inst().get_now());
		}
	}
	
	public void work(String farmID, String dongID, String comein_time){
		String query = "SELECT be.beStatus, be.beAvgWeightDate, fd.fdType, IFNULL(sf.sfDate, 'no') AS feeder FROM buffer_sensor_status AS be" 
				+ 		" JOIN farm_detail AS fd ON fd.fdFarmid = be.beFarmid AND fd.fdDongid = be.beDongid"
				+		" LEFT JOIN set_feeder AS sf ON sf.sfFarmid = be.beFarmid AND sf.sfDongid = be.beDongid "
				+ 		" WHERE beFarmid = '" + farmID + "' AND beDongid = '" + dongID + "';";
		
		List<HashMap<String, Object>> info = MysqlConn.get_sql().select(query, new String[]{"beStatus", "beAvgWeightDate", "fdType", "feeder"});
		
		if(info.size() < 1){
			FileUtil.write("ERROR => set_comein fail / has no data in " + farmID + " " + dongID);
			return;
		}
		
		String status = (String) info.get(0).get("beStatus");
		String type = (String) info.get(0).get("fdType");
		String feeder = (String) info.get(0).get("feeder");
		String last_avg_date = (String) info.get(0).get("beAvgWeightDate");
		switch (status) {
		
		case "O":	//출하 일경우 입추처리
			OtoI(farmID, dongID, comein_time, type, feeder);
			
			break;
			
		case "E":	//에러 상태인 경우
			EtoI(farmID, dongID, last_avg_date);
			
			break;
			
		case "W":	//대기 중인 경우 솎기 처리
			WtoI(farmID, dongID, last_avg_date);
			
			break;
		}
	}
}
