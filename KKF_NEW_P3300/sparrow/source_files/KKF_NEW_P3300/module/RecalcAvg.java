package module;

import java.util.HashMap;
import java.util.List;

import org.bson.Document;
import org.json.simple.JSONObject;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

import kokofarm.FarmInfo;
import util.FileUtil;
import util.MongoConn;
import util.DateUtil;
import util.MysqlConn;

//********************************************************
//class		recalculate_avg
//role		평균중량 재산출
//********************************************************
public class RecalcAvg implements Runnable {
	public RecalcAvg(String m_farmID, String m_dongID, String m_start_time, String m_end_time) {
		farmID = m_farmID;
		dongID = m_dongID;
		start_time = m_start_time;
		end_time = m_end_time;
	}
	
	public RecalcAvg(String m_farmID, String m_dongID, String m_start_time, String m_end_time, String m_request_date, boolean m_request) {
		farmID = m_farmID;
		dongID = m_dongID;
		start_time = m_start_time;
		end_time = m_end_time;
		request_date = m_request_date;
		is_request = m_request;
	}
	
	String farmID = "";
	String dongID = "";
	String start_time = "";
	String end_time = "";
	String request_date = "";
	
	boolean is_request = false;
	
	int cnt_complete = 0;
	int cnt_error = 0;
	
	HashMap<String, List<Document>> data_map = new HashMap<String, List<Document>>();
	//HashMap<String, Double> corr_map = new HashMap<String, Double>();
	
	@SuppressWarnings("unchecked")
	public void run() {
		//time = 2020-07-17 15:27:00   /   length = 19
		start_time = start_time.substring(0, 15) + "0:00";
		
		String now_time = DateUtil.get_inst().get_now();
		int now_minute = Integer.parseInt(now_time.substring(15, 16));
		int now_second = Integer.parseInt(now_time.substring(17, 19));
		
		if(now_minute < 5) {		//0~4분인 경우
			end_time = DateUtil.get_inst().get_plus_minus_minute_time(now_time, -10).substring(0, 15) + "0:00";
		}
		else {						//5~9분인 경우
			end_time = now_time.substring(0, 15) + "0:00";
		}
		
		if(now_minute == 5 && now_second <= 30) {		//5분이면 원 평균중량 산출과 겹치지 않기 위해 1분동안 대기함
			try {
				Thread.sleep((31 - now_second) * 1000);		// 5분 31초가 될때까지 대기
			} catch (InterruptedException e) {
				FileUtil.write("ERROR => Recalculate Thread Sleep Error");
			} catch (Exception e){
				FileUtil.write("ERROR => Recalculate Thread Sleep Error");
			}
		}
		
		FileUtil.write("Start => Recalculate " + farmID + " " + dongID + " Time : " + start_time + " ~ " + end_time);
		
		String base_type = "";
		String base_date = "";
		
		long s_stamp = DateUtil.get_inst().get_timestamp(start_time);
		long e_stamp = DateUtil.get_inst().get_timestamp(end_time);
		
		if(s_stamp < e_stamp) {		// 시간이 같거나 시작시간이 더 큰 경우 스킵
			
			//재산출 구간 평균중량 데이터가 있으면 삭제
			MysqlConn.get_sql().delete("avg_weight", "awFarmid = '"+ farmID +"' AND awDongid = '"+ dongID +"' AND awDate BETWEEN '"+ start_time +"' AND '"+ end_time +"'");
			
			// 일령 기준 시간 선택
			String query = "SELECT cmIntype, cmIndate FROM comein_master WHERE cmFarmid = '" + farmID + "' AND cmDongid = '" + dongID + "' ORDER BY cmIndate DESC LIMIT 1";
			List<HashMap<String, Object>> comein_info = MysqlConn.get_sql().select(query, new String[]{"cmIntype", "cmIndate"});
			
			// 데이터 오류 방지
			if(comein_info.size() < 1){
				FileUtil.write("ERROR => start_recalculate fail recalculate end / has no data in " + farmID + " " + dongID);
				
				FarmInfo.list_map.get(farmID + dongID).set_is_recalc(false);
				return;
			}
			
			base_type = (String) comein_info.get(0).get("cmIntype");
			base_date = (String) comein_info.get(0).get("cmIndate");
			
			//데이터가 아예없을 경우 재산출 종료
			if(base_date == null || base_type == null || base_date.isEmpty() || base_type.isEmpty()) {
				FileUtil.write("ERROR => start_recalculate fail recalculate end / has no data in " + farmID + " " + dongID);
				
				FarmInfo.list_map.get(farmID + dongID).set_is_recalc(false);
				return;
			}
			
			//하루 단위로 시간 분리
			int total_days = DateUtil.get_inst().get_total_days(start_time, end_time);
			String temp_start = start_time;
			String temp_end = "";
			
			//백업 데이터 재산출 대상 데이터 불러오기
			AggregateIterable<Document> output = null;
			
			cnt_complete = 0;
			cnt_error = 0;
			
			for(int i=1; i<=total_days; i++) {				//재산출 데이터를 날짜별로 잘라서 재산출함
				
				if(i == total_days) {		//시작 시간과 종료시간의 날짜가 같을 때
					temp_end = end_time;
				}
				else {						//시작 시간과 종료시간의 날짜가 다를 때
					temp_end = DateUtil.get_inst().get_midNight(temp_start);
				}
				
				// 2021-02-22 이병선 수정 - 재산출 마지막 시간이 00:00:00초인 경우를 방지- exception 발생함 
				if(temp_start.equals(temp_end)){
					break;
				}
				
				output = MongoConn.get_mongo().aggregate_for_avg("recalculate", temp_start, temp_end, farmID, dongID);
				temp_start = temp_end;		//다음 for문에서 사용
				
				//aggregate 데이터 파싱
				MongoCursor<Document> it = output.iterator();
				
				while(it.hasNext()) {
					Document row = it.next();
					String getTime = row.getString("_id");
					
					if(getTime.equals("err")) {
						continue;
					}
					
					data_map.put(getTime, (List<Document>) row.get("data"));
					
				} // while
			}// for
			
			String while_time = start_time;
			while(!while_time.equals(end_time)) {
				int day = DateUtil.get_inst().get_days(base_date, while_time);
				
				if(day <= 0) {		//백업 데이터가 입추 이전 데이터 일 경우 
					while_time = DateUtil.get_inst().get_plus_minus_minute_time(while_time, 10);
					cnt_error++; 
					continue;
				}
				
				String send = "";
				if(data_map.containsKey(while_time)) {		// 데이터 있는 경우
					send = AvgCalcBridge.get_inst().make_avg_json( data_map.get(while_time), farmID, dongID, base_type, while_time, day);
				}
				else {		// 데이터 없는 경우
					send = AvgCalcBridge.get_inst().make_empty_avg_json(farmID, dongID, base_type, while_time, day);
				}
				
				// 정상 산출 로직에서 사용중이면 대기
				while(AvgCalcBridge.get_inst().is_orgin_run){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						FileUtil.write("ERROR => InterruptedException avg_calc_bridge.is_orgin_run wait error");
					} catch (Exception e){
						FileUtil.write("ERROR => Exception Thread avg_calc_bridge.is_orgin_run wait error");
					}
				}
				
				JSONObject result = AvgCalcBridge.get_inst().get_avg_calc(send, while_time); 
				
				if(((String)result.get("success")).equals("ok")){ 
					insert_avg(result);
				}
				else{ 
					cnt_error++; 
				}
				
				while_time = DateUtil.get_inst().get_plus_minus_minute_time(while_time, 10);
			}
			
			//2020-11-06 이병선 재산출 후 버퍼테이블 평균중량 업데이트 시간 변경 -> e, w 상태에서 백업 수신 시 중복 재산출 안되게 하기 위함
			HashMap<String, String> update_map = new HashMap<String, String>();
			update_map.put("beAvgWeightDate", DateUtil.get_inst().get_plus_minus_minute_time(end_time, -10));
			MysqlConn.get_sql().update("buffer_sensor_status", "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'", update_map);
			
			FileUtil.write("COMPLETE => Recalculate " + farmID + " " + dongID + " Complete => " + cnt_complete + " Err => " + cnt_error );
			
		}
		else {
			FileUtil.write("COMPLETE => Recalculate " + farmID + " " + dongID + " None");
		}
		
		// 요청인 경우에만 진행 - 작업 상태를 완료로 변경하고, 완료 시간을 입력
		if(is_request){
			HashMap<String, String> c_map = new HashMap<String, String>();
			c_map.put("rcStatus", "F");
			c_map.put("rcFinishDate", DateUtil.get_inst().get_now());
			
			String where = "rcFarmid = '" + farmID + "' AND rcDongid = '" + dongID + "' AND rcRequestDate = '" + request_date + "'";
			
			MysqlConn.get_sql().update("request_calculate", where, c_map);
			
			FileUtil.write("COMPLETE => " + farmID + " " + dongID + " change request status : F");
		}
		
		//재산출 완료 후 원래 상태로 되돌림
		FarmInfo.list_map.get(farmID + dongID).set_is_recalc(false);
	}
	
	@SuppressWarnings("unchecked")
	private void insert_avg(JSONObject jo) {
		try {
			
			List<Long> nDis_list = (List<Long>) jo.get("nDis");
			String beNdis = Long.toString(nDis_list.get(0));
			for(int n=1; n<nDis_list.size(); n++) {
				beNdis += "|" + Long.toString(nDis_list.get(n));
			}
			
			//avg_weight 적재
			HashMap<String, String> insert_map = new HashMap<String, String>();
			insert_map.put("awFarmid", 		(String) jo.get("farmID"));
			insert_map.put("awDongid", 		(String) jo.get("dongID"));
			insert_map.put("awDate", 		(String) jo.get("getTime"));
			insert_map.put("awWeight", 		Double.toString((double) jo.get("avgWeight")));
			insert_map.put("awDevi", 		Double.toString((double) jo.get("stdDevi")));
			insert_map.put("awVc", 			Double.toString((double) jo.get("vcVal")));
			insert_map.put("awEstiT1", 		Double.toString((double) jo.get("estiT1")));
			insert_map.put("awEstiT2", 		Double.toString((double) jo.get("estiT2")));
			insert_map.put("awEstiT3", 		Double.toString((double) jo.get("estiT3")));
			insert_map.put("awDays", 		(String) jo.get("Days"));
			insert_map.put("awNdis", 		beNdis);
			
			MysqlConn.get_sql().insert("avg_weight", insert_map);
			cnt_complete++;
			
		}catch(NullPointerException e) {
			cnt_error++;
			FileUtil.write("ERROR => Error insert_avg in Recalculate " + farmID + " " + dongID);
		}catch(NumberFormatException e) {
			cnt_error++;
			FileUtil.write("ERROR => Error insert_avg in Recalculate " + farmID + " " + dongID);
		}
	}
}
