package module;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import kokofarm.FarmInfo;
import kokofarm.SensorUnit;
import util.FileUtil;
import util.MysqlConn;

public class BufferMerger implements Runnable{
	
	private static final String[] BUFFER_KEY_ARR = new String[]{"beIPaddr", "beSensorDate", "beAvgTemp", "beAvgHumi", "beAvgCo2", "beAvgNh3", "beAvgDust"};
	private static final String[] SENSOR_KEY_ARR = new String[]{"siSensorDate", "siTemp", "siHumi", "siCo2", "siNh3", "siDust", "siWeight"};
	
	List<FarmInfo> list = new ArrayList<FarmInfo>();
	
	String pklog = "";
	
	public BufferMerger(List<FarmInfo> m_list) {
		list = m_list;
	}

	@Override
	public void run() {
		try {
			update_buffer(list);
			update_cell(list);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		pklog = pklog.substring(0, pklog.length() - 2);
		FileUtil.write("COMPLETE => buffer_sensor_status Update Count : " + list.size() + " --- " + pklog);
	}
	
	private void update_buffer(List<FarmInfo> update_taget_list) {			//쿼리문을 만들어 buffer_table_update_queue에 적재
		
		// buffer_sensor_status 업데이트
		
		String buffer_update_query = "UPDATE buffer_sensor_status SET ";
		String where_query = " WHERE CONCAT(beFarmid, beDongid) IN (";
		HashMap<String, String> case_map = new HashMap<String, String>();
		
		for(FarmInfo info: update_taget_list){
			HashMap<String, String> map = info.get_buffer_data();
			
			String pk = map.get("beFarmid") + map.get("beDongid");
			where_query += "'" + pk + "', ";
			
			pklog += pk + ", ";
			
			for(String key : BUFFER_KEY_ARR){
				String case_str = "";
				if(case_map.containsKey(key)){
					case_str = case_map.get(key);
				}
				else{
					case_str = key + " = CASE CONCAT(beFarmid, beDongid) ";
				}
				
				case_str += "WHEN '" + pk + "' THEN '" + map.get(key) + "' ";
				
				case_map.put(key, case_str);
			}
		}
		
		for(Entry<String, String> entry : case_map.entrySet()){
			buffer_update_query += entry.getValue() + " ELSE " + entry.getKey() + " END, ";
		}
		buffer_update_query = buffer_update_query.substring(0, buffer_update_query.length() - 2);
		where_query = where_query.substring(0, where_query.length() - 2);
		where_query += ")";
		
		buffer_update_query += where_query;
		
		//FileUtil.write("test => buffer_sensor_status length : " + buffer_update_query.length() );
		query(buffer_update_query, "");
		//P3300Listner.update_executors.execute(new RunSql(buffer_update_query, "COMPLETE => buffer_sensor_status Update Count : " + update_taget_list.size()));
	}
	
	private void update_cell(List<FarmInfo> update_taget_list) {			//쿼리문을 만들어 set_iot_cell 업데이트
		
		// buffer_sensor_status 업데이트
		
		String buffer_update_query = "UPDATE set_iot_cell SET ";
		String where_query = " WHERE CONCAT(siFarmid, siDongid, siCellid) IN (";
		HashMap<String, String> case_map = new HashMap<String, String>();
		
		for(FarmInfo info: update_taget_list){
			
			for(Entry<String, SensorUnit> entry : info.get_cell_map().entrySet()){
				SensorUnit cell = entry.getValue();
				
				String pk = cell.get_farm_id() + cell.get_dong_id() + cell.get_cell_id();
				where_query += "'" + pk + "', ";
				
				for(String key : SENSOR_KEY_ARR){
					String case_str = "";
					if(case_map.containsKey(key)){
						case_str = case_map.get(key);
					}
					else{
						case_str = key + " = CASE CONCAT(siFarmid, siDongid, siCellid) ";
					}
					
					case_str += "WHEN '" + pk + "' THEN '" + cell.get_sql_val(key) + "' ";
					
					case_map.put(key, case_str);
				}
				
			}
		}
		
		for(Entry<String, String> entry : case_map.entrySet()){
			buffer_update_query += entry.getValue() + " ELSE " + entry.getKey() + " END, ";
		}
		buffer_update_query = buffer_update_query.substring(0, buffer_update_query.length() - 2);
		where_query = where_query.substring(0, where_query.length() - 2);
		where_query += ")";
		
		buffer_update_query += where_query;
		
		//FileUtil.write("test => set_iot_cell length : " + buffer_update_query.length() );
		query(buffer_update_query, "");
		//P3300Listner.update_executors.execute(new RunSql(buffer_update_query, "COMPLETE => set_iot_cell Update Count : " + update_taget_list.size()));
	}
	
	private void query(String sql, String log) {
		Statement state = MysqlConn.get_sql().get_statement();
		try {
			if(state != null) {
				state.executeUpdate(sql);		//업데이트 수행
			}
			
			if(!log.equals("")){
				FileUtil.write(log);
			}
			
		} catch (SQLException e) {
			FileUtil.write("ERROR => MySql Update SQLException / BufferMerger / " + sql);
		} catch (Exception e) {
			FileUtil.write("ERROR => MySql Update Exception / BufferMerger / " + sql);
		}
		
		try {
			state.close();
		} catch (SQLException e) {
			FileUtil.write("ERROR => MySql Close SQLException / BufferMerger / " + sql);
		}
	}
}
