package module;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kokofarm.FarmInfo;
import util.MysqlConn;

public class CheckRecalc {
	private static CheckRecalc inst = null;
	
	public static CheckRecalc get_inst(){
		if(inst == null){
			inst = new CheckRecalc();
		}
		
		return inst;
	}
	
	private ExecutorService recalc_executors = Executors.newSingleThreadExecutor();
	
	private CheckRecalc(){}
	
	public void start_recalculate(String farm, String dong, String start, String end){
		HashMap<String, String> update_map = new HashMap<String, String>();
		update_map.put("beSensorDate", end);		//마지막 적재시간을 마지막 측정시간으로 변경
		
		String where = "beFarmid = '"+ farm +"'" + " AND beDongid = '"+ dong + "'";
		MysqlConn.get_sql().update("buffer_sensor_status", where, update_map);
		
		// 재산출에 등록
		FarmInfo.list_map.get(farm + dong).set_is_recalc(true);
		recalc_executors.execute(new RecalcAvg(farm, dong, start, end));
		
	}
	
}
