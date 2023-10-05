package plc_object;

import org.bson.Document;

import util.DateUtil;

public class BitInfo {

	private Integer addr;
	private Integer bit;
	private String property;
	private String name;
	private String remark;
	
	public BitInfo(Integer addr, Integer bit, String property, String name, String remark) {
		this.addr = addr;
		this.bit = bit;
		this.property = property;
		this.name = name;
		this.remark = remark;
	}
	
	public Integer get_addr() {
		return addr;
	}
	public Integer get_bit() {
		return bit;
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
	
	public Integer get_where() {
		return addr * 16 + bit;
	}
	
	public Document get_event_doc(String farm, String dong, String get_time, long time_stamp, Object val) {
		Document ret = new Document();
		int milisec = DateUtil.get_inst().get_now_nano() / 1000000;
		String id = farm + dong + "b" + addr + "-" + bit + "_" + time_stamp + "_" + String.format("%03d", milisec);
		
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
