package kokofarm;

import org.bson.Document;

public class Device {
	
	protected String farm_id;
	protected String dong_id;
	//protected HashMap<String, Object> values;
	protected Document doc;
	
	protected String table_name;
	
	protected Device(){
		doc = new Document();
		//values = new HashMap<String, Object>();
	}
	
	public Object get_val(String key){
		Object ret = doc.get(key);
		if(ret == null){
			ret = 0;
		}
		
		return ret;
	}
	
	public Double get_double(String key) {
		Double ret;
		try {
			ret = doc.getDouble(key);
		} catch (Exception e) {
			ret = null;
		}
		
		if(ret == null){
			ret = 0.0;
		}
		
		return ret;
	}
	
	public String get_string(String key) {
		String ret;
		try {
			ret = doc.getString(key);
		} catch (Exception e) {
			ret = null;
		}
		
		if(ret == null){
			ret = "";
		}
		
		return ret;
	}
	
	public void clear(){
		doc.clear();
	}
	
	public void set_val(String key, Object val){
		doc.append(key, val);
	}
	
//	public String get_sensor_date(){
//		return sensor_date;
//	}
//	
//	public void set_sensor_date(String val){
//		sensor_date = val;
//	}
	
	public String get_farm_id(){
		return farm_id;
	}
	
	public String get_dong_id(){
		return dong_id;
	}
	
	public Document get_doc() {
		return doc;
	}
	
}
