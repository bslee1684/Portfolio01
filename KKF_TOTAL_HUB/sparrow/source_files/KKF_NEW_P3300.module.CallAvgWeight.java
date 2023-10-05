package module;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.json.simple.JSONObject;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

import kokofarm.FarmInfo;

import java.sql.Statement;

import util.FileUtil;
import util.MongoConn;
import util.DateUtil;
import util.MysqlConn;

//********************************************************
//class		call_avg_weight
//role		timer 관리, buffer_tabel_list 제공
//			버퍼테이블 업데이트, 평균중량 산출 알고리즘 호출, 쿼리문 작성
//********************************************************
public class CallAvgWeight {
	private static CallAvgWeight m = new CallAvgWeight();
	
	private CallAvgWeight(){
		
	}
	
	public static CallAvgWeight get_inst() {
		return m;
	}
	
	static final String[] FIELDS = new String[]{"beDays", "beAvgWeightDate", "beAvgWeight", "beDevi", "beVc", "beEstiT1", "beEstiT2", "beEstiT3", "beNdis"};
	
	ScheduledExecutorService correct_timer_service = null;
	
	// then case 업데이트 문 사용
	String buffer_update_query = "";
	String buffer_update_where = "";
	HashMap<String, String> stage_map = new HashMap<String, String>();
	
	int cnt_complete;
	int cnt_error;
	
    public void start() {
    	
    	//**************************************************************
    	// avg_timer 초단위 교정 타이머
    	//**************************************************************
    	Runnable correct_timer = new Runnable() {
			@Override
			public void run() {
				String now_second = DateUtil.get_inst().get_now_simple().substring(12);
				if(now_second.equals("30")) {		//현재 초가 30초면 평균중량 산출 타이머를 실행
					avg_timer();
					correct_timer_service.shutdownNow();
				}
			}
    	};
    	correct_timer_service = Executors.newSingleThreadScheduledExecutor();
    	correct_timer_service.scheduleAtFixedRate(correct_timer, 0, 1000, TimeUnit.MILLISECONDS);
        
    }//start
    
    private void avg_timer() {
    	FileUtil.write("START => Avg Weight Timer Start");
    	
    	//**************************************************************
    	// 평균중량 산출 타이머
    	//**************************************************************
        Runnable avg_weight_timer = new Runnable() {
            @Override
            public void run() {
            	String avg_now_time = DateUtil.get_inst().get_now();		//현재시간을 가져옴
        		int now_minute = Integer.parseInt(avg_now_time.substring(14, 16));		//현재 시간의 분을 얻어옴
        		
        		//현재시간이 5분, 15분, 25분, 35분, 45분, 55분 인경우
        		if(now_minute%10 == 5) {
        			
        			String start = DateUtil.get_inst().get_plus_minus_minute_time(avg_now_time, -15).substring(0, 16) + ":00";
        			String end = DateUtil.get_inst().get_plus_minus_minute_time(avg_now_time, -6).substring(0, 16) + ":59";
        			
        			FileUtil.write("START => AVG Calculating Start / " + start + " ~ " + end);
        			
        			// 정상 산출 로직에서 사용한다고 알림
        			AvgCalcBridge.get_inst().is_orgin_run = true;
        			AvgCalcBridge.get_inst().last_origin_time = start;
        			
        			//평균중량 산출 대상 가져오기
        			HashMap<String, Integer> days_map = new HashMap<String, Integer>();
        			HashMap<String, String> type_map = new HashMap<String, String>();
        			HashMap<String, Double> corr_map = new HashMap<String, Double>();
        			
        			try {
        				
        				//2020-11-06 이병선 보정 관련 추가 쿼리
        				String prev_date = DateUtil.get_inst().get_plus_minus_minute_time(start, -1440);
						
        				String query = "SELECT be.beFarmid, be.beDongid, cm.cmIntype, "
        								+ "cm.cmIndate, IFNULL(aw.awWeight, 0) AS prevWeight FROM buffer_sensor_status AS be "
        								+ "JOIN comein_master AS cm ON cm.cmCode = be.beComeinCode "
        								+ "LEFT JOIN avg_weight AS aw ON beFarmid = aw.awFarmid AND beDongid = aw.awDongid AND aw.awDate = '" + prev_date + "' "
        								+ "WHERE beStatus = 'I'";
        				
						Statement state = MysqlConn.get_sql().get_statement();
						if(state != null) {
							ResultSet set = state.executeQuery(query);
							while(set.next()) {
								String farmID = set.getString("beFarmid");
								String dongID = set.getString("beDongid");
								String type = set.getString("cmIntype");
								Double corr = set.getDouble("prevWeight");
								
								String cm_indate = set.getString("cmIndate");		//시차일령
								//cm_indate = cm_indate.length() > 19 && cm_indate != null ? cm_indate.substring(0, 19) : "";
								
								int days = !cm_indate.equals("") ? DateUtil.get_inst().get_days(cm_indate, end) : -1;
								
								if(farmID != null && dongID != null && days != -1 && type != null && corr >= 0) {
									days_map.put(farmID + dongID, days);
									type_map.put(farmID + dongID, type);
									corr_map.put(farmID + dongID, corr);
								}
							}
							
							set.close();
							state.close();
						}
						
					} catch (SQLException e1) {
						FileUtil.write("ERROR => Avg List call Error void return");
						return;
					} catch (Exception e1) {
						FileUtil.write("ERROR => Avg List call Error void return");
						return;
					}
        			
        			//----------------------------------------------------------
                	buffer_update_query = "UPDATE buffer_sensor_status SET ";
                	buffer_update_where = " WHERE CONCAT(beFarmid, beDongid) IN (";
                	
                	stage_map.clear();
                	for(String field : FIELDS){
                		stage_map.put(field, field + " = CASE CONCAT(beFarmid, beDongid) ");
                	}
        			
        			//10분간의 데이터를 몽고DB에서 aggregate
        			AggregateIterable<Document> output = MongoConn.get_mongo().aggregate_for_avg("avg_weight", start, end);
        			
        			//aggregate 데이터 파싱
        			MongoCursor<Document> it = output.iterator();
    				
        			cnt_complete = 0;
        			cnt_error = 0;
    				while(it.hasNext()) {	//it.hasNext()
    					try {
							//****************************************************************************
							// Document를 순차적으로 가져와서 평균중량 알고리즘을 호출함
							// Aggregate된 Document 형식 : 	_id						data
							//							farmID	dongID			List<Integer>
							//****************************************************************************
							Document row = it.next();
							Document f_key = (Document) row.get("_id");
							String farmID = f_key.getString("farmID");
							String dongID = f_key.getString("dongID");
							
							//해당 동이 재산출 상태인 경우
							FarmInfo farminfo = FarmInfo.list_map.get(farmID + dongID);
							if(farminfo != null) {
								if(farminfo.get_recalc_status()) {
									days_map.remove(farmID + dongID);
									type_map.remove(farmID + dongID);
									corr_map.remove(farmID + dongID);
									FileUtil.write("SKIP => " + farmID + " " + dongID + " is Recalculating. Avg Calculating Skipped");
									continue;
								}
							}
							
							//현재 아직 출하중 인경우 스킵 -> 입추 데이터 10분 대기 중인 경우 -> 데이터는 있는데 입추목록에 없는 경우
							if(!days_map.containsKey(farmID + dongID)) {
								continue;
							}
							
							int day = days_map.get(farmID + dongID);
							
							if(day != -1) {
								@SuppressWarnings("unchecked")
								String send = AvgCalcBridge.get_inst().make_avg_json( (List<Document>) row.get("data"), farmID, dongID, type_map.get(farmID + dongID), start, day);
								JSONObject result = AvgCalcBridge.get_inst().get_avg_calc(send, start); 
								
								//System.out.println("result => " + result.toJSONString());
								
								if(((String)result.get("success")).equals("ok")){ 
									insert_avg(result);
								}
								else{ 
									cnt_error++; 
								}
							}
							else{
								cnt_error++;
							}
							
							days_map.remove(farmID + dongID);
							type_map.remove(farmID + dongID);
							corr_map.remove(farmID + dongID);
						} catch (NullPointerException e) {
							FileUtil.write("END => NullPointerException in it.hasNext() call_avg_weight");
						}
    					
    				} //while
    				
    				//데이터가 있는 동을 모두 인서트 한 후 데이터가 없는 동은 
    				for(String key : days_map.keySet()) {
    					try {
							String farmID = key.substring(0, 6);
							String dongID = key.substring(6);
							
							//해당 동이 재산출 상태인 경우
							FarmInfo farminfo = FarmInfo.list_map.get(farmID + dongID);
							if(farminfo != null) {
								if(farminfo.get_recalc_status()) {
									FileUtil.write("SKIP => " + farmID + " " + dongID + " is Recalculating. Avg Calculating Skipped");
									continue;
								}
							}
							
							int day = days_map.get(farmID + dongID);
							
							if(day != -1) {
								JSONObject result = AvgCalcBridge.get_inst().send_empty(farmID, dongID, type_map.get(key), start, day);
								
								if(((String) result.get("success")).equals("ok")){
									insert_avg(result);
								}
								else{
									cnt_error++;
								}
							}
							else{
								cnt_error++;
							}
							
						} catch (NullPointerException e) {
							FileUtil.write("END => Exception in days_map.keySet() call_avg_weight");
						}
    				}
    				
    				try {
    					if(cnt_complete > 0) {
    						
    						for(Entry<String, String> entry : stage_map.entrySet()){
    							buffer_update_query += entry.getValue() + " ELSE " + entry.getKey() + " END, ";
    						}
    						
    						buffer_update_query = buffer_update_query.substring(0, buffer_update_query.length() - 2);
    						buffer_update_where = buffer_update_where.substring(0, buffer_update_where.length() - 2);
    						buffer_update_where += ")";
    						
    						buffer_update_query += buffer_update_where;
            				
            				//FileUtil.write(buffer_avg_update_query);
            				
            				//업데이트 쿼리 전송
            				Statement state = MysqlConn.get_sql().get_statement();
            				if(state != null) {
            					state.executeUpdate(buffer_update_query);
            					state.close();
            				}
    					}
    					
    					FileUtil.write("END => AVG Calculating End");
        				FileUtil.write("RESULT => Avg cnt_complete : " + cnt_complete + " cnt_error : " + cnt_error);
        				
    				}catch(SQLException e) {
    					e.printStackTrace();
        				FileUtil.write("ERROR => AvgWeight Update Error");
        			}catch(Exception e) {
        				e.printStackTrace();
        				FileUtil.write("ERROR => AvgWeight Update Error");
        			}
    				
    				// 정상 산출 로직에서 사용을 마쳤다고 알림
        			AvgCalcBridge.get_inst().is_orgin_run = false;
        		}
        	
            }
        };
        ScheduledExecutorService avg_weight_timer_service = Executors.newSingleThreadScheduledExecutor();
        avg_weight_timer_service.scheduleAtFixedRate(avg_weight_timer, 0, 60000, TimeUnit.MILLISECONDS);
	}
    
	private void insert_avg(JSONObject jo) {
    	try {
    		//{"beDays", "beAvgWeightDate", "beAvgWeight", "beDevi", "beVc", "beEstiT1", "beEsitT2", "beEstiT3", "beNdis"};
			//***************************************************************************
			//버퍼테이블 평균중량 업데이트 쿼리 만들기
			//***************************************************************************
			String id = (String) jo.get("farmID") + (String) jo.get("dongID");
			
			//리스트로 받아서 문자열로 변환하여 업데이트
			@SuppressWarnings("unchecked")
			List<Long> nDis_list = (List<Long>) jo.get("nDis");
			String beNdis = Long.toString(nDis_list.get(0));
			for(int n=1; n<nDis_list.size(); n++) {
				beNdis += "|" + Long.toString(nDis_list.get(n));
			}
			
			HashMap<String, String> value_map = new HashMap<String, String>();
			value_map.put("beDays", 			(String) jo.get("Days"));
			value_map.put("beAvgWeightDate", 	(String) jo.get("getTime"));
			value_map.put("beAvgWeight", 		Double.toString((double) jo.get("avgWeight")));
			value_map.put("beDevi", 			Double.toString((double) jo.get("stdDevi")));
			value_map.put("beVc", 				Double.toString((double) jo.get("vcVal")));
			value_map.put("beEstiT1", 			Double.toString((double) jo.get("estiT1")));
			value_map.put("beEstiT2", 			Double.toString((double) jo.get("estiT2")));
			value_map.put("beEstiT3", 			Double.toString((double) jo.get("estiT3")));
			value_map.put("beNdis", 			beNdis);
			
			for(String field : FIELDS){
				String stage_query = stage_map.get(field) + " WHEN '" + id + "' THEN '" + value_map.get(field) + "'";
        		stage_map.put(field, stage_query);
        	}
			
			buffer_update_where += "'" + id + "', ";
			
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
			FileUtil.write("ERROR => NullPointerException in AvgWeight Timer");
		}catch(NumberFormatException e) {
			cnt_error++;
			FileUtil.write("ERROR => NullPointerException in AvgWeight Timer");
		}
    }
    
}
