package kokofarm;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

import com.mongodb.MongoException;

import p3300.P3300Listner;
import run_object.RunSql;
import util.FileUtil;
import util.MongoConn;

public class LightSensor extends Device {

	public static ConcurrentHashMap<String, LightSensor> list_map = new ConcurrentHashMap<String, LightSensor>();
	
	private FarmInfo info;
	
	public static LightSensor get_inst(String m_farm, String m_dong, FarmInfo m_info) {
		
		if(list_map.containsKey(m_farm + m_dong)){
			return list_map.get(m_farm + m_dong);
		}
		
		LightSensor instance = new LightSensor(m_farm, m_dong, m_info);
		
		list_map.put(m_farm + m_dong, instance);
		
		return instance;
	}
	
	private LightSensor(String m_farm, String m_dong, FarmInfo m_info){
		farm_id = m_farm;
		dong_id = m_dong;
		doc = new Document();
		
		info = m_info;
	}
	
	public void update() {
		
		try {
			MongoConn.get_mongo().get_db().getCollection("lightSensor").insertOne(doc);
		} catch (MongoException e) {
			FileUtil.write("ERROR => MongoException in LightSensor : " + e.getMessage());
		}
		
		HashMap<String, String> update_map = new HashMap<String, String>();
		update_map.put("slFarmid", farm_id);
		update_map.put("slDongid", dong_id);
		update_map.put("slSensorDate", (String) get_val("getTime"));
		update_map.put("slLight01", Integer.toString((int) get_val("light01")));
		update_map.put("slLight02", Integer.toString((int) get_val("light02")));
		update_map.put("slLight03", Integer.toString((int) get_val("light03")));
		update_map.put("slLight04", Integer.toString((int) get_val("light04")));
		
		String s_table = "set_light";
		String s_where = "slFarmid = '" + farm_id + "' AND slDongid = '" + dong_id + "'";
		P3300Listner.update_executors.execute(new RunSql(s_table, s_where, update_map, ""));
		
		info.put_history("light_01", (int) get_val("light01"));
		info.put_history("light_02", (int) get_val("light02"));
		info.put_history("light_03", (int) get_val("light03"));
		info.put_history("light_04", (int) get_val("light04"));
	}
}
