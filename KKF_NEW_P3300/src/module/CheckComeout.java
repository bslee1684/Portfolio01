package module;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kokofarm.FarmInfo;
import p3300.P3300Listener;
import run_object.RunCamera;
import run_object.RunSql;
import util.FileUtil;
import util.DateUtil;
import util.MysqlConn;

public class CheckComeout {
	private static CheckComeout inst = new CheckComeout();
	
	private CheckComeout(){
		inout_time_error = (long) FileUtil.get_config("inout_time_error");
		inout_time_wait = (long) FileUtil.get_config("inout_time_wait");
		inout_time_out = (long) FileUtil.get_config("inout_time_out");
	}
	
	public static CheckComeout get_inst() {
		return inst;
	}
	
	//config
	long inout_time_error = 0;			//동 오류 판단 시간
	long inout_time_wait = 0;			//동 출하 대기 상태 시간
	long inout_time_out = 0;			//동 출하 처리 시간
	
	public void start() {
		
		FileUtil.write("START => Check Out Timer Start");
		//**************************************************************
    	// 입추, 출하 판단 프로세스
    	//**************************************************************
        Runnable in_out_timer = new Runnable() {
			@Override
            public void run() {
				// 출하 농장 제외한 후 가져옴
				String check_sql = "SELECT be.beFarmid, be.beDongid, be.beIPaddr, be.beStatus, be.beSensorDate, be.beComeinCode, "
								 + "cm.cmIndate, cm.cmIsManual, c.cName2 AS semiDay, c.cName3 AS allDay, sc.scPort, sc.scUrl "
								 + "FROM buffer_sensor_status AS be "
								 + "JOIN comein_master AS cm ON cm.cmCode = be.beComeinCode "
								 + "JOIN codeinfo AS c ON c.cGroup = '출하방지' AND c.cName1 = cm.cmIntype "
								 + "LEFT JOIN set_camera AS sc ON sc.scFarmid = be.beFarmid AND sc.scDongid = be.beDongid "
								 + "WHERE beStatus != 'O'";
				
				int farm_cnt = 0;
				
				Statement state = MysqlConn.get_sql().get_statement();
				if(state != null) {
					
					long now_stamp = DateUtil.get_inst().get_now_timestamp();		//현재 시간
					
					ResultSet set = null;
					
					try {
						set = state.executeQuery(check_sql);
					}catch (SQLException e) {
						FileUtil.write("ERROR => SQLException Check Query Error / Class : check_come_out / func : run() " + check_sql);
					}
						
					try {
						while(set.next()) {		//여기서 처리
							
							farm_cnt++;
							
							String beStatus = set.getString("beStatus");
							String farmID = set.getString("beFarmid");
							String dongID = set.getString("beDongid");
							
							// 2023-05-12 수동 입추 처리된 농장은 자동 출하 하지 않음
							String cmIsManual = set.getString("cmIsManual");
							if(cmIsManual.equals("y")) {
								FileUtil.write("SKIP => " + farmID + " " + dongID + " is Manual. Come Out Check Skipped");
								continue;
							}
							
							//해당 동이 재산출 상태인 경우
							FarmInfo farminfo = FarmInfo.list_map.get(farmID + dongID);
							if(farminfo != null) {
								if(farminfo.get_recalc_status()) {
									FileUtil.write("SKIP => " + farmID + " " + dongID + " is Recalculating. Come Out Check Skipped");
									continue;
								}
							}
							
							HashMap<String, String> update_map = new HashMap<String, String>();
							int interm = 0;
							
							// 입추시간 가져오기
							String comein_time = set.getString("cmIndate");
							//comein_time = comein_time.length() > 19 ? comein_time.substring(0, 19) : "";
							
							// 최종 수집시간 가져오기
							String last_time = set.getString("beSensorDate");
							//last_time = last_time.length() > 19 ? last_time.substring(0, 19) : "";
							long last_stamp = DateUtil.get_inst().get_timestamp(last_time);
							
							// 현재시간 - 최종수집시간 차이
							long now_last_gap = now_stamp - last_stamp;
							
							//시간 차이는 전부 초로 계산 함
							switch (beStatus) {
							
							case "I":		//입추상태일 경우
								
								//에러 상태 변화 확인 로직
								if(now_last_gap > inout_time_error) {		//현재시간과 최종 적재 시간 차이가 10분 이상이면
									// status를 E로 변경
									update_map.clear();
									String s_where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
									update_map.put("beStatus", "E");
									MysqlConn.get_sql().update("buffer_sensor_status", s_where, update_map);
									
									//카메라 이미지 저장
									String s_ip = set.getString("beIPaddr");
									String s_port = set.getString("scPort");
									String s_url = set.getString("scUrl");
									
									if(s_ip != null && !s_ip.isEmpty()){
										P3300Listener.camera_executors.execute(new RunCamera(s_ip, s_port, s_url, farmID + "/" + dongID, "E"));
									}
									
									//RDB에 기록 저장
									update_map.clear();
									update_map.put("ccFarmid", farmID);
									update_map.put("ccDongid", dongID);
									update_map.put("ccCapDate", DateUtil.get_inst().get_now());
									update_map.put("ccStatus", "E");
									update_map.put("ccPrvDate", last_time);
									
									P3300Listener.update_executors.execute(new RunSql("capture_camera", update_map, "COMPLETE => Capture Data Insert Completed " + farmID + " " + dongID));
									
									FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'I' to 'E' Gap : " + now_last_gap + " Second");
								}
								
								break;
								
							case "E":		//오류 상태일 경우
								//출하 대기 상태 변화 로직
								interm = DateUtil.get_inst().get_days(comein_time);		//시차일령
								
								String semi_day = set.getString("semiDay");
								int inout_day_semi = semi_day != null ? Integer.parseInt(semi_day) : 20;
								
								if(interm >= inout_day_semi) {		//20일령 이상일 경우
									if(now_last_gap > inout_time_wait) {		//현재시간과 최종 적재 시간 차이가 4시간 이상이면
										String s_where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
										update_map.clear();
										update_map.put("beStatus", "W");
										MysqlConn.get_sql().update("buffer_sensor_status", s_where, update_map);
										
										FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'E' to 'W' Gap : " + now_last_gap + " Second");
									}
								}
								
								break;
								
							case "W":		//출하 대기 상태일 경우
								//출하 상태 변화 로직
								interm = DateUtil.get_inst().get_days(comein_time);		//시차일령
								
								String all_day = set.getString("allDay");
								int inout_day_all = all_day != null ? Integer.parseInt(all_day) : 20;
								if(interm >= inout_day_all) {//30일령 이상일 경우
									if(now_last_gap > inout_time_out) {		//현재시간과 최종 적재 시간 차이가 24시간 이상이면
										String s_where = "beFarmid = '"+ farmID +"'" + " AND beDongid = '"+ dongID + "'";
										update_map.clear();
										update_map.put("beStatus", "O");
										MysqlConn.get_sql().update("buffer_sensor_status", s_where, update_map);
										FileUtil.write("ALARM => " + farmID + " " + dongID + " Status Change 'W' to 'O' Gap : " + now_last_gap + " Second");
										
										//comin_master 출하 처리
										update_map.clear();
										String comein_code = set.getString("beComeinCode");
										s_where = "cmCode = '"+ comein_code +"'";
										update_map.put("cmOutdate", last_time);
										MysqlConn.get_sql().update("comein_master", s_where, update_map);
										
										//출하 시간 후 현재시간까지 평균중량 데이터 삭제
										MysqlConn.get_sql().delete("avg_weight", "awFarmid = '"+ farmID +"' AND awDongid = '"+ dongID +"' AND awDate BETWEEN '"+ last_time +"' AND '"+ DateUtil.get_inst().get_now() +"'");
										
										
										FileUtil.write("COMPLETE => " + farmID + " " + dongID + " Come Out");
									}
								}
								
								break;	

							default:
								break;
								
							}
							
							//FileUtil.write("CHECK => " + farmID + " "  + dongID +" days : " + days + " semi : " + Inout_day_semi + " all : " + Inout_day_all);
							
						} // while
						
						set.close();
						state.close();
					} catch (NumberFormatException e) {
						FileUtil.write("ERROR => NumberFormatException in check_come_out while");
					} catch (SQLException e) {
						FileUtil.write("ERROR => SQLException in check_come_out while");
					}
					
					FileUtil.write("END => Check come out End farm_cnt : " + farm_cnt);
					
					//FileUtil.write("CHECK => info : " + FarmInfo.list_map.size() + " unit : " + SensorUnit.list_map.size() + " extern : " + ExternSensor.list_map.size() + " light : " + LightSensor.list_map.size());
					
				} // if statment != null
				
            } // run
        };
        ScheduledExecutorService test_timer_service = Executors.newSingleThreadScheduledExecutor();
        test_timer_service.scheduleAtFixedRate(in_out_timer, 60000, 60000, TimeUnit.MILLISECONDS);
	}
	
}
