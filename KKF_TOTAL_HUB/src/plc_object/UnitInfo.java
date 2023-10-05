package plc_object;

import org.bson.Document;

import util.DateUtil;
import util.FloatCompute;

public class UnitInfo {
	
	private Integer addr;
	private String property;
	private String name;
	private String remark;
	
	private boolean is_signed = true;
	private int point = 1;
	
	public UnitInfo(Integer addr, String property, String name, String remark, String rule) {
		this.addr = addr;
		this.property = property;
		this.name = name;
		this.remark = remark;
		
		try {
			String[] token = rule.split("-");
			
			if(token[0].equals("unsigned")) {
				this.is_signed = false;
			}
			
			point = Integer.parseInt(token[1]);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Integer get_addr() {
		return addr;
	}
	public String get_property() {
		return property;
	}
	public String get_name() {
		return name;
	}
	public String get_remark() {
		return remark;
	}
	
	public double get_rule_val(int val) {		// 받는 수는 
		
		if(!is_signed) {
			val = val < 0 ? val + 65536 : val;
		}
		
		double ret = FloatCompute.divide(val, Math.pow(10, point));
		
		return ret;
	}
	
	public double get_rule_val(byte high, byte low) {
		
		int val = 0;
		if(is_signed) {
			val = (high << 8) | (low & 0xFF);
		}
		else {
			val = ((high & 0xFF) << 8) | (low & 0xFF);
		}
		
		double ret = FloatCompute.divide(val, Math.pow(10, point));
		
		return ret;
	}
	
	public Document get_event_doc(String farm, String dong, String get_time, long time_stamp, Object val) {
		
		Document ret = new Document();
		int milisec = DateUtil.get_inst().get_now_nano() / 1000000;
		String id = farm + dong + addr + "_" + time_stamp + "_" + String.format("%03d", milisec);
		
		ret.append("_id", id);
		ret.append("farmID", farm);
		ret.append("dongID", dong);
		ret.append("unitID", addr);
		ret.append("getTime", get_time);
		ret.append("uProperty", property);
		ret.append("uName", name);
		ret.append("uRemark", remark);
		ret.append("uStatus", val);
		
		return ret;
	}
}
