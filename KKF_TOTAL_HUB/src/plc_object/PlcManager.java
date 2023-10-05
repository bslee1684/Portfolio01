package plc_object;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import mini_pc.PlcBridge;
import mini_pc.MiniClient;
import util.DateUtil;
import util.FileUtil;
import util.MysqlConn;
import util.SimpleTimer;

/*------------------------------------------------------------------------------
 * PLC 연결 및 데이터 관리
 * - PLC 접속정보를 set_plc 테이블에서 가져옴
 * - word 주소 정보를 set_plc_unitid 테이블에서 가져옴
 * - bit 주소 정보를 set_plc_bit 테이블에서 가져옴
 * - 1분마다 PLC 연결을 갱신
 * - 1분마다 저울 및 사육 데이터를 가져와서 PLC로 넘겨줌
-------------------------------------------------------------------------------*/

public class PlcManager {
	private static PlcManager inst = null;
	
	public static PlcManager get_inst() {
		if(inst == null) {
			inst = new PlcManager();
		}
		
		return inst;
	}
	
	private SimpleTimer timer = null;
	public ConcurrentHashMap<String, PlcBridge> plc_bridge_map = new ConcurrentHashMap<String, PlcBridge>();
	public HashMap<Integer, UnitInfo> unit_info_map = new HashMap<Integer, UnitInfo>();
	public HashMap<Integer, BitInfo> bit_info_map = new HashMap<Integer, BitInfo>();
	public LinkedHashMap<String, List<Integer>> sensor_table_map = new LinkedHashMap<String, List<Integer>>();
	
	public static ExecutorService mysql_executor = Executors.newCachedThreadPool();
	
	// 평균중량 등 버퍼데이터 참조 맵
//	public ConcurrentHashMap<String, List<Integer>> buffer_table_map = new ConcurrentHashMap<String, List<Integer>>();
	
	public static UnitInfo get_unit_info(int addr) {
		if(inst == null) {
			return null;
		}
		
		return inst.unit_info_map.getOrDefault(addr, null);
	}
	
	public static BitInfo get_bit_info(int addr) {
		if(inst == null) {
			return null;
		}
		
		return inst.bit_info_map.getOrDefault(addr, null);
	}
	
	public static boolean is_work(String farmID, String dongID) {
		if(inst == null) {
			return false;
		}
		
		PlcBridge plc = inst.plc_bridge_map.getOrDefault(farmID + dongID, null);
		if(plc != null && plc.is_connect()) {
			return true;
		}
		
		return false;
	}
	
	public static boolean contains_info(int addr) {
		if(inst == null) {
			return false;
		}
		
		return inst.unit_info_map.containsKey(addr);
	}
	
	private PlcManager() {
		FileUtil.write("CREATE => PlcManager Created");
		timer = SimpleTimer.create_inst();
	}
	
	public void start() {
		
		get_device_info();
		get_bit_info();
		
		timer.stop();
		timer.set_runnable(new Runnable() {
			
			@Override
			public void run() {
				
				get_plc_info_map();
				
				for(PlcBridge plc : plc_bridge_map.values()) {
					if(!plc.is_connect()) {
						plc.start();
					}
				}
			}
		});
		timer.start(0, 1000 * 60);
	}
	
	private void get_device_info() {
		try {
			String query = "SELECT * FROM set_plc_unitid";
			
			Statement state = MysqlConn.get_sql().get_statement();
			if(state != null) {
				ResultSet set = state.executeQuery(query);
				while(set.next()) {
					Integer addr = set.getInt("suAddr");
					String property = set.getString("suProperty");
					String name = set.getString("suName");
					String remark = set.getString("suRemark");
					String rule = set.getString("suParseRule");
					
					UnitInfo unit = new UnitInfo(addr, property, name, remark, rule);
					unit_info_map.put(addr, unit);
					
					if(property.equals("S")) {
						if(!sensor_table_map.containsKey(name)) {
							sensor_table_map.put(name, new ArrayList<Integer>());
						}
						
						sensor_table_map.get(name).add(addr);
					}
				}
				
				set.close();
				state.close();
			}
			
		} catch (SQLException e1) {
			FileUtil.write("ERROR => SQLException in get_device_info");
			return;
		} catch (Exception e1) {
			FileUtil.write("ERROR => Exception in get_device_info");
			return;
		}
	}
	
	private void get_bit_info() {
		try {
			String query = "SELECT * FROM set_plc_bit";
			
			Statement state = MysqlConn.get_sql().get_statement();
			if(state != null) {
				ResultSet set = state.executeQuery(query);
				while(set.next()) {
					Integer addr = set.getInt("sbAddr");
					Integer bit = set.getInt("sbBit");
					String property = set.getString("sbProperty");
					String name = set.getString("sbName");
					String remark = set.getString("sbRemark");
					
					BitInfo bitInfo = new BitInfo(addr, bit, property, name, remark);
					bit_info_map.put(bitInfo.get_where(), bitInfo);
				}
				
				set.close();
				state.close();
			}
			
		} catch (SQLException e1) {
			FileUtil.write("ERROR => SQLException in get_bit_info");
			return;
		} catch (Exception e1) {
			FileUtil.write("ERROR => Exception in get_bit_info");
			return;
		}
	}
	
	private void get_plc_info_map() {
		
		long start_stamp = DateUtil.get_inst().get_now_timestamp();
//		FileUtil.write("START => get_plc_info");
		
		try {	
//			String query = "SELECT sp.*, f.fPW, IFNULL(beAvgWeight, 0.0) AS beAvgWeight, IFNULL(beDevi, 0.0) AS beDevi, IFNULL(beVc, 0.0) AS beVc FROM set_plc AS sp ";
//			query += "JOIN farm AS f ON f.fFarmid = sp.spFarmid ";
//			query += "LEFT JOIN buffer_sensor_status AS be ON be.beFarmid = sp.spFarmid AND be.beDongid = sp.spDongid ";
			
			String query = "SELECT sp.*, f.fPW, IFNULL(beAvgWeight, 0.0) AS beAvgWeight, IFNULL(beDevi, 0.0) AS beDevi, IFNULL(beVc, 0.0) AS beVc, ";
			query += "IFNULL(siTemp, '') AS siTemp, IFNULL(siHumi, '') AS siHumi, IFNULL(siCo2, '') AS siCo2, IFNULL(siNh3, '') AS siNh3, ";
			query += "IFNULL(cmCode, '') AS cmCode, IFNULL(cmInsu, 0) AS cmInsu, IFNULL(cmDeathCount, 0) AS cmDeathCount, IFNULL(cmCullCount, 0) AS cmCullCount, IFNULL(cmThinoutCount, 0) AS cmThinoutCount, ";
			query += "IFNULL(cdDeath, 0) AS cdDeath, IFNULL(cdCull, 0) AS cdCull, IFNULL(cdThinout, 0) AS cdThinout ";
			query += "FROM set_plc AS sp ";
			query += "JOIN farm AS f ON f.fFarmid = sp.spFarmid ";
			query += "LEFT JOIN buffer_sensor_status AS be ON be.beFarmid = sp.spFarmid AND be.beDongid = sp.spDongid ";
			query += "LEFT JOIN comein_master AS cm ON cm.cmCode = be.beComeinCode ";
			query += "LEFT JOIN comein_detail AS cd ON cm.cmCode = cd.cdCode AND cd.cdDate = DATE_FORMAT(now(), '%Y-%m-%d') ";
			query += "LEFT JOIN (";
			query += "	SELECT siFarmid, siDongid, ";
			query += "		GROUP_CONCAT(TRUNCATE(siTemp, 1) separator ' / ') AS siTemp, GROUP_CONCAT(TRUNCATE(siHumi, 1) separator ' / ') AS siHumi, ";
			query += "		GROUP_CONCAT(TRUNCATE(siCo2, 1) separator ' / ') AS siCo2, GROUP_CONCAT(TRUNCATE(siNh3, 1) separator ' / ') AS siNh3";
			query += "    FROM set_iot_cell GROUP BY siFarmid, siDongid";
			query += ") AS si ON si.siFarmid = sp.spFarmid AND si.siDongid = sp.spDongid";
			
			Statement state = MysqlConn.get_sql().get_statement();
			if(state != null) {
				ResultSet set = state.executeQuery(query);
				while(set.next()) {
					String farmID = set.getString("spFarmid");
					String dongID = set.getString("spDongid");
					String url = set.getString("spURL");
					
//					if(!farmID.equals("KF0006")) {
//						continue;
//					}
					
					// plc 클라이언트 연결 맵
					if(!plc_bridge_map.containsKey(farmID + dongID)) {
						
						String isAI = set.getString("spAI");
						
						PlcBridge plc = null;
						
						if(isAI.equals("y")) {
							plc = new MiniClient(url, 600 + Integer.parseInt(dongID), farmID, dongID);
						}
						else {
							plc = new PlcClient(url, 600 + Integer.parseInt(dongID), farmID, dongID);
						}
						
						plc_bridge_map.put(farmID + dongID, plc);
					}
					
					PlcBridge plc = plc_bridge_map.get(farmID + dongID);
					
					String passwd = set.getString("spPW");
					plc.set_passwd(passwd);
					
					int[] data_arr = new int[70];
					
					// plc로 전송할 데이터 가공
					double beAvgWeight = set.getDouble("beAvgWeight");
					double beDevi = set.getDouble("beDevi");
					double beVc = set.getDouble("beVc");
					
					data_arr[0] = (int) Math.round(beAvgWeight * 10);		// 9011 - 평균중량
					data_arr[2] = (int) Math.round(beDevi * 10);
					data_arr[3] = (int) Math.round(beVc * 10);
					
					String cmCode = set.getString("cmCode");
					plc.set_comein_code(cmCode);
					
					int cmInsu = set.getInt("cmInsu");
					int cdDeath = set.getInt("cdDeath");
					int cdCull = set.getInt("cdCull");
					int cdThinout = set.getInt("cdThinout");
					int cmDeathCount = set.getInt("cmDeathCount");
					int cmCullCount = set.getInt("cmCullCount");
					int cmThinoutCount = set.getInt("cmThinoutCount");
					
					plc.set_breed_map(9021, cmInsu);
					plc.set_breed_map(9022, cdDeath);
					plc.set_breed_map(9023, cdCull);
					plc.set_breed_map(9024, cdThinout);
//					plc.set_breed_map(9025, cmDeathCount);
//					plc.set_breed_map(9026, cmCullCount);
//					plc.set_breed_map(9027, cmThinoutCount);
					
					data_arr[10] = cmInsu;									// 9021 - 입추수
					data_arr[11] = cdDeath;
					data_arr[12] = cdCull;
					data_arr[13] = cdThinout;
					data_arr[14] = cmDeathCount;
					data_arr[15] = cmCullCount;
					data_arr[16] = cmThinoutCount;							// 9027 - 누적 솎기수
					
					String[] temp_token = set.getString("siTemp").split("/");
					String[] humi_token = set.getString("siHumi").split("/");
					String[] co2_token = set.getString("siCo2").split("/");
					String[] nh3_token = set.getString("siNh3").split("/");
					
					// 저울 센서 데이터 넘기기
					if(temp_token.length > 0 && !temp_token[0].equals("")) {
						
						int len = temp_token.length > 6 ? 6 : temp_token.length;
						
						for(int i=0; i<len; i++) {
							data_arr[i*5 + 41] = (int) Math.round(Float.parseFloat(temp_token[i].trim()) * 10); 
							data_arr[i*5 + 42] = (int) Math.round(Float.parseFloat(humi_token[i].trim()) * 10); 
							data_arr[i*5 + 43] = (int) Math.round(Float.parseFloat(co2_token[i].trim()) * 10); 
							data_arr[i*5 + 44] = (int) Math.round(Float.parseFloat(nh3_token[i].trim()) * 10); 
						}
					}
					
					plc.send_word(9011, data_arr);
				}
				
				set.close();
				state.close();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			FileUtil.write("ERROR => SQLException in get_plc_map");
			return;
		} catch (Exception e) {
			e.printStackTrace();
			FileUtil.write("ERROR => Exception in get_plc_map");
			return;
		}
		
		long end_stamp = DateUtil.get_inst().get_now_timestamp();
//		FileUtil.write("END => get_plc_info --- term : " + (end_stamp - start_stamp));
	}
}
