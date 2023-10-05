package kokofarm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.mongodb.MongoException;

import module.BufferUpdate;
import module.CheckComein;
import module.CheckRecalc;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.HashBuffer;
import util.MongoConn;
import util.MysqlConn;

public class FarmInfo {
	
	public static FarmInfo get_inst(String farm, String dong){
		
		if(list_map.containsKey(farm + dong)){
			return list_map.get(farm + dong);
		}
		
		FarmInfo instance = new FarmInfo(farm, dong);
		list_map.put(farm + dong, instance);
		
		return instance;
	}
	
//	private static final String[] BUFFER_KEY_ARR = new String[]{"beIPaddr", "beSensorDate", "beAvgTemp", "beAvgHumi", "beAvgCo2", "beAvgNh3", "beAvgDust"};
//	private static final String[] SENSOR_KEY_ARR = new String[]{"siSensorDate", "siTemp", "siHumi", "siCo2", "siNh3", "siDust", "siWeight"};
	
	// id가 세팅된 인스턴스 맵
	public static ConcurrentHashMap<String, FarmInfo> list_map = new ConcurrentHashMap<String, FarmInfo>();
	static final int CHECK_UPDATE_COUNT = (int) (long)FileUtil.get_config("check_in_count");						//입추 기준 - 10개
	static final long CHECK_TIME_BACKUP = (long)FileUtil.get_config("check_time_backup");					//백업 판단 시간
	
	private String farm_id = "";
	private String dong_id = "";
	private String ip_addr = "";
	
	boolean is_backup = false;
	boolean is_recalc = false;
	
	String last_sensor_date = "";													//최종 데이터 수집 시간
	String first_update_date = "";													//데이터 최초 적재 시간 - in_check 가 1일 때 시간
	String backup_start_date = "";													//백업 시작시간
	
	Long last_update_stamp = DateUtil.get_inst().get_now_timestamp();				//최종 데이터 업데이트 시간		-- my_timer에 의해서 실행됨
	Long last_work_stamp = DateUtil.get_inst().get_now_timestamp();					
	
	int update_count = 0;
	
	private HashMap<String, SensorUnit> sensor_unit_map = null;
	private ExternSensor extern_sensor = null;
	private LightSensor light_sensor = null;
	
	private List<Document> sensor_data_list = new ArrayList<Document>();
	
	private HashBuffer history_buffer = new HashBuffer();
	
	private FarmInfo(String farm, String dong){
		farm_id = farm;
		dong_id = dong;
		sensor_unit_map = new HashMap<String, SensorUnit>();
	}
	
	public void delete_inst(){
		list_map.remove(farm_id + dong_id);
	}
	
	public SensorUnit get_cell(String cell_id){
		if(!sensor_unit_map.containsKey(cell_id)){
			sensor_unit_map.put(cell_id, SensorUnit.get_inst(farm_id, dong_id, cell_id));
		}
		return sensor_unit_map.get(cell_id);
	}
	
	public HashMap<String, SensorUnit> get_cell_map(){
		return sensor_unit_map;
	}
	
	public ExternSensor get_extern_sensor() {
		if(extern_sensor == null){
			extern_sensor = ExternSensor.get_inst(farm_id, dong_id, this);
		}
		return extern_sensor;
	}
	
	public boolean has_extern_sensor(){
		if(extern_sensor == null){
			return false;
		}
		return true;
	}
	
	public LightSensor get_light_sensor() {
		if(light_sensor == null){
			light_sensor = LightSensor.get_inst(farm_id, dong_id, this);
		}
		return light_sensor;
	}
	
	public boolean has_light_sensor(){
		if(light_sensor == null){
			return false;
		}
		return true;
	}

//	public void set_extra_sensor(ext_sensor extra_sensor) {
//		this.extra_sensor = extra_sensor;
//	}

	public String get_last_sensor_date() {
		return last_sensor_date;
	}

	public void set_last_sensor_date(String last_sensor_date) {
		this.last_sensor_date = last_sensor_date;
	}

	public String get_farm_id() {
		return farm_id;
	}
	
	public String get_dong_id() {
		return dong_id;
	}
	
	public String get_ip_addr(){
		return ip_addr;
	}
	
	public void set_ip_addr(String ip_addr){
		this.ip_addr = ip_addr;
	}
	
	public void set_is_recalc(boolean is_recalc){
		this.is_recalc = is_recalc;
	}
	
	public void put_history(String key, double val){
		history_buffer.put(key, val);
	}
	
	public HashBuffer get_history_buffer(){
		return history_buffer;
	}
	
	public void add_sensor_data_list(Document dc) {
		sensor_data_list.add(dc);
	}
	
	public void work() {
		
		// 저울 데이터 없으면 동작 안함
		if(sensor_data_list.size() <= 0) {
			return;
		}
		
		// 몽고db 업데이트 
		run_mongo_update();
		
		// 백업 판단 로직
		long last_sensor_stamp = DateUtil.get_inst().get_timestamp(last_sensor_date);
		last_work_stamp = DateUtil.get_inst().get_now_timestamp();
		
		if(!is_backup){		//백업 아닌경우
			if(last_work_stamp - last_sensor_stamp > CHECK_TIME_BACKUP){			// 데이터를 판단 해서 백업이면
				backup_start_date = last_sensor_date;
				
				is_backup = true;
				FileUtil.write("START => Back Up Start " + farm_id + " " + dong_id);
			}
			else {			// 백업이 아니면 정상로직
				//run_update_buffer();
				//run_update_cell();
				BufferUpdate.update_queue.offer(this);
				
				//입추 처리 로직
				if(update_count < CHECK_UPDATE_COUNT) {		//데이터 적재 횟수가 입추 기준 횟수보다 작으면
					update_count++;
					
					if(update_count == 1){
						first_update_date = last_sensor_date;		// 첫 업데이트 시간에 계측 시간 데이터를 기록
					}
					
					if(update_count == CHECK_UPDATE_COUNT){
						if(last_work_stamp - DateUtil.get_inst().get_timestamp(first_update_date) < 780){		// 10번의 데이터가 13분 내에 들어온 경우
							CheckComein.get_inst().work(farm_id, dong_id, first_update_date);
						}
						else{
							update_count = 0;
						}
					}
				}
			}
		}
		else {			// 백업인 경우 
			if(last_work_stamp - last_sensor_stamp < 180) {		// 백업 데이터가 다 들어왔으면
				run_backup_end();
			}
		}
	}
	
	public void run_mongo_update() {
		// sensorData 적재
		List<Document> copy_list = new ArrayList<Document>();
		for (int i = 0; i < sensor_data_list.size(); i++) {
			copy_list.add(sensor_data_list.get(i));
		}
		
		try {
			MongoConn.get_mongo().get_db().getCollection("sensorData").insertMany(copy_list);
		} catch (MongoException e) {
			FileUtil.write("ERROR => MongoException in FarmInfo : " + e.getMessage());
		}	
		sensor_data_list.clear();
	}
	
//	private void run_update_buffer() {			//쿼리문을 만들어 buffer_table_update_queue에 적재
//		
//		// buffer_sensor_status 업데이트
//		
//		String buffer_update_query = "UPDATE buffer_sensor_status SET ";
//		String where_query = " WHERE CONCAT(beFarmid, beDongid) IN (";
//		HashMap<String, String> case_map = new HashMap<String, String>();
//		
//		HashMap<String, String> map = get_buffer_data();
//		
//		String pk = map.get("beFarmid") + map.get("beDongid");
//		where_query += "'" + pk + "', ";
//		
//		for(String key : BUFFER_KEY_ARR){
//			String case_str = "";
//			if(case_map.containsKey(key)){
//				case_str = case_map.get(key);
//			}
//			else{
//				case_str = key + " = CASE CONCAT(beFarmid, beDongid) ";
//			}
//			
//			case_str += "WHEN '" + pk + "' THEN '" + map.get(key) + "' ";
//			
//			case_map.put(key, case_str);
//		}
//		
//		for(Entry<String, String> entry : case_map.entrySet()){
//			buffer_update_query += entry.getValue() + " ELSE " + entry.getKey() + " END, ";
//		}
//		buffer_update_query = buffer_update_query.substring(0, buffer_update_query.length() - 2);
//		where_query = where_query.substring(0, where_query.length() - 2);
//		where_query += ")";
//		
//		buffer_update_query += where_query;
//		
//		//FileUtil.write("test => buffer_sensor_status length : " + buffer_update_query.length() );
//		P3300Listner.update_executors.execute(new RunSql(buffer_update_query, "COMPLETE => buffer_sensor_status Update : " + farm_id + " " + dong_id));
//	}
//	
//	private void run_update_cell() {			//쿼리문을 만들어 set_iot_cell 업데이트
//		
//		// buffer_sensor_status 업데이트
//		
//		
//		String buffer_update_query = "UPDATE set_iot_cell SET ";
//		String where_query = " WHERE CONCAT(siFarmid, siDongid, siCellid) IN (";
//		HashMap<String, String> case_map = new HashMap<String, String>();
//		
//		for(Entry<String, SensorUnit> entry : sensor_unit_map.entrySet()){
//			SensorUnit cell = entry.getValue();
//			
//			String pk = cell.get_farm_id() + cell.get_dong_id() + cell.get_cell_id();
//			where_query += "'" + pk + "', ";
//			
//			for(String key : SENSOR_KEY_ARR){
//				String case_str = "";
//				if(case_map.containsKey(key)){
//					case_str = case_map.get(key);
//				}
//				else{
//					case_str = key + " = CASE CONCAT(siFarmid, siDongid, siCellid) ";
//				}
//				
//				case_str += "WHEN '" + pk + "' THEN '" + cell.get_sql_val(key) + "' ";
//				
//				case_map.put(key, case_str);
//			}
//		}
//		
//		for(Entry<String, String> entry : case_map.entrySet()){
//			buffer_update_query += entry.getValue() + " ELSE " + entry.getKey() + " END, ";
//		}
//		buffer_update_query = buffer_update_query.substring(0, buffer_update_query.length() - 2);
//		where_query = where_query.substring(0, where_query.length() - 2);
//		where_query += ")";
//		
//		buffer_update_query += where_query;
//		
//		//FileUtil.write("test => set_iot_cell length : " + buffer_update_query.length() );
//		P3300Listner.update_executors.execute(new RunSql(buffer_update_query, "COMPLETE => set_iot_cell Update : " + farm_id + " " + dong_id));
//	}
	
	public HashMap<String, String> get_buffer_data(){
		HashMap<String, String> ret = new HashMap<String, String>();
		
		ret.put("beFarmid", farm_id);
		ret.put("beDongid", dong_id);
		ret.put("beIPaddr", ip_addr);
		
		// 적재된지 5분이상 지났으면 삭제 (현재 동작이 멈춘 저울의 데이터를 삭제함)
		for(Entry<String, SensorUnit> entry : sensor_unit_map.entrySet()){
			SensorUnit cell = entry.getValue();
			String sensor_date = (String) cell.get_val("getTime");
			long sensor_stamp = DateUtil.get_inst().get_timestamp(sensor_date);
			
			if(DateUtil.get_inst().get_now_timestamp() - sensor_stamp > 300){			
				cell.clear();
			}
		}
		
		double avg_temp = get_sensor_avg("temp");
		double avg_humi = get_sensor_avg("humi");
		double avg_co2 = get_sensor_avg("co");
		double avg_nh3 = get_sensor_avg("nh");
		double avg_dust = get_sensor_avg("dust");
		
		ret.put("beSensorDate", DateUtil.get_inst().get_now());
		ret.put("beAvgTemp", String.format("%.1f", avg_temp));
		ret.put("beAvgHumi", String.format("%.1f", avg_humi));
		ret.put("beAvgCo2", String.format("%.1f", avg_co2));
		ret.put("beAvgNh3", String.format("%.1f", avg_nh3));
		ret.put("beAvgDust", String.format("%.1f", avg_dust));
		
		// 히스토리 적재
		history_buffer.put("cell_temp", avg_temp);
		history_buffer.put("cell_humi", avg_humi);
		history_buffer.put("cell_co2", avg_co2);
		history_buffer.put("cell_nh3", avg_nh3);
		history_buffer.put("cell_dust", avg_dust);
		
		return ret;
		
	}
	
	private double get_sensor_avg(String key){
		double avg = 0.0;
		int cnt = 0;
		for(Entry<String, SensorUnit> entry : sensor_unit_map.entrySet()){
			SensorUnit cell = entry.getValue();
			try {
				double value = (double) cell.get_val(key);
				if(value > 0){
					avg = FloatCompute.plus(avg, value);
					cnt++;
				}
				
			} catch (NumberFormatException e) {
				FileUtil.write("ERROR => get_sensor_avg " + key + " in " + farm_id + " " + dong_id);;
			}
		}
		
		if(cnt > 0){
			avg = FloatCompute.divide(avg, cnt);
		}
		
		return avg;
	}
	
	private void run_backup_end() {
		is_backup = false;
		
		String query = "SELECT beStatus, beAvgWeightDate FROM buffer_sensor_status WHERE beFarmid = '" + farm_id + "' AND beDongid = '" + dong_id + "'";
		
		List<HashMap<String, Object>> result = MysqlConn.get_sql().select(query, new String[]{"beStatus", "beAvgWeightDate"});
		
		String last_avg_date = (String) result.get(0).get("beAvgWeightDate");
		String status = (String) result.get(0).get("beStatus");
		
		if(status != "O"){
			is_recalc = true;
			CheckRecalc.get_inst().start_recalculate(farm_id, dong_id, last_avg_date, last_sensor_date);
		}
		
		FileUtil.write("END => Back Up End " + farm_id + " " + dong_id);
	}
	
	public void check() {
		long now_stamp = DateUtil.get_inst().get_now_timestamp();
		if(is_backup) {
			if(now_stamp - last_work_stamp > 240) {
				run_backup_end();
			}
		}
		else {		
			// 데이터 안들어오는 센서 유닛 확인 로직
		}
	}
	
	public boolean get_recalc_status(){
		if(is_backup){
			return true;
		}
		
		if(is_recalc){
			return true;
		}
		
		return false;
	}
}
