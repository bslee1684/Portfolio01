package kokofarm;

import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;

public class SensorUnit extends Device {
	
	public static ConcurrentHashMap<String, SensorUnit> list_map = new ConcurrentHashMap<String, SensorUnit>();
	
	private String cell_id;
	private int max_weight;

	public static SensorUnit get_inst(String m_farm, String m_dong, String m_cell) {
		
		if(list_map.containsKey(m_farm + m_dong + m_cell)){
			return list_map.get(m_farm + m_dong + m_cell);
		}
		
		SensorUnit instance = new SensorUnit(m_farm, m_dong, m_cell);
		
		list_map.put(m_farm + m_dong + m_cell, instance);
		
		return instance;
	}
	
	private SensorUnit(String m_farm, String m_dong, String m_cell){
		farm_id = m_farm;
		dong_id = m_dong;
		cell_id = m_cell;
		doc = new Document();
	}
	
	public String get_cell_id(){
		return cell_id;
	}
	
	public void set_max_weight(int max_weight) {
		this.max_weight = max_weight;
	}
	
	@Override
	public void clear(){
		for(String key : doc.keySet()){
			if(!key.equals("getTime")){		// 시간만 유지
				doc.put(key, 0.0);
			}
		}
	}
	
	// {"siSensorDate", "siTemp", "siHumi", "siCo2", "siNh3", "siWeight"};
	public Object get_sql_val(String key) {
		
		Object ret = "";
		
		switch (key) {
		case "siSensorDate":
			ret = get_val("getTime");
			break;
		case "siTemp":
			ret = get_val("temp");
			break;
		case "siHumi":
			ret = get_val("humi");
			break;
		case "siCo2":
			ret = get_val("co");
			break;
		case "siNh3":
			ret = get_val("nh");
			break;
		case "siDust":
			ret = get_val("dust");
			break;
		case "siWeight":
			ret = max_weight;
			break;
		}
		
		return ret;
	}
}
