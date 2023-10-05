package module;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import kokofarm.FarmInfo;
import util.FileUtil;
import util.MysqlConn;
import util.DateUtil;

public class RequestWorker {
	private static RequestWorker inst = null;
	ScheduledExecutorService timer = null;
	
	//대기열
	ConcurrentHashMap<String, HashMap<String, String>> request_info_map = new ConcurrentHashMap<String, HashMap<String, String>>();
	List<String> wait_list = Collections.synchronizedList(new ArrayList<String>());
	
	ExecutorService socket_excutor = null;
	ExecutorService recalculate_excutor = null;
	
	String curr_id = "";
	
	public static RequestWorker get_inst(){
		if(inst == null){
			inst = new RequestWorker();
		}
		
		return inst;
	}
	
	// 생성 제한
	private RequestWorker(){
		FileUtil.write("CREATE => request_worker Created");
	}
	
	public void start(){
		timer_run(new run_read(), 1000 * 30, 1000 * 30);		
	}
	
	// 타이머 재설정 함수
	public void timer_run(Runnable runnable, int delay, int rate){
		if(timer != null){
			timer.shutdownNow();
			timer = null;
		}
		
		timer = Executors.newSingleThreadScheduledExecutor();
		timer.scheduleAtFixedRate(runnable, delay, rate, TimeUnit.MILLISECONDS);
	}
	
	private void change_request_status(String farm, String dong, String request_date, String c_status){
		HashMap<String, String> c_map = new HashMap<String, String>();
		c_map.put("rcStatus", c_status);
		
		String where = "rcFarmid = '" + farm + "' AND rcDongid = '" + dong + "' AND rcRequestDate = '" + request_date + "'";
		
		MysqlConn.get_sql().update("request_calculate", where, c_map);
		
		FileUtil.write("COMPLETE => " + farm + " " + dong + " change request status : " + c_status);
	}
	
	// 요청을 읽어옴
	private class run_read implements Runnable{
		
		public void run(){
			// 30일 이내의 데이터만 찾아옴
			String date_minus_30 = DateUtil.get_inst().get_plus_minus_minute_time(DateUtil.get_inst().get_now(), -30 * 1440);
			
			String read_sql = "SELECT * FROM request_calculate WHERE rcRequestDate > '" + date_minus_30 + "' AND rcStatus = 'A';";
			
			//FileUtil.write(read_sql);
			
			Statement state = MysqlConn.get_sql().get_statement();
			if(state != null) {
				
				ResultSet set = null;
				
				try {
					set = state.executeQuery(read_sql);
					
					while(set.next()) {
						
						String rcFarmid = set.getString("rcFarmid");
						String rcDongid = set.getString("rcDongid");
						String rcRequestDate = set.getString("rcRequestDate");
						
						System.out.println(rcRequestDate);
						
						HashMap<String, String> rc_map = new HashMap<String, String>();
						
						rc_map.put("rcFarmid", rcFarmid);
						rc_map.put("rcDongid", rcDongid);
						rc_map.put("rcRequestDate", rcRequestDate);
						
						rc_map.put("rcCode", set.getString("rcCode"));
						rc_map.put("rcCommand", set.getString("rcCommand"));
						rc_map.put("rcChangeLst", set.getString("rcChangeLst"));
						rc_map.put("rcChangeDate", set.getString("rcChangeDate"));
						
						rc_map.put("rcMeasureDate", set.getString("rcMeasureDate"));
						rc_map.put("rcMeasureVal", set.getString("rcMeasureVal"));
						
						// 롤백관련
						Double rcChangeRatio = set.getDouble("rcChangeRatio");
						if(rcChangeRatio <= 0.0){
							rc_map.put("rcChangeRatio", rcChangeRatio.toString());
						}
						
						// 대기열에 존재하는지 확인 - 존재 시 삭제
						if(wait_list.contains(rcFarmid + rcDongid)){
							wait_list.remove(rcFarmid + rcDongid);
							
							//대기열에 저장된 산출 요청 명령이 중복되어 삭제되면 I(중단) 상태로 변경
							change_request_status(rcFarmid, rcDongid, request_info_map.get(rcFarmid + rcDongid).get("rcRequestDate"), "I");
							request_info_map.remove(rcFarmid + rcDongid);
						}
						
						wait_list.add(rcFarmid + rcDongid);
						request_info_map.put(rcFarmid + rcDongid, rc_map);
						
						// 대기열에 저장 후 W(wait) 상태로 변환
						change_request_status(rcFarmid, rcDongid, rcRequestDate, "W");
						
					}
					
				} catch (SQLException e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Select SQLException " + read_sql + " / Class : request_worker");
				} catch (Exception e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Select Exception " + read_sql + " / Class : request_worker");
				}
				
				try {
					if(set != null) {
						set.close();
					}
					
					if(state != null) {
						state.close();
					}
				} catch (SQLException e) {
					FileUtil.write("ERROR => SQLException in request_worker ");;
				}
			}
			
			if(curr_id.equals("") && wait_list.size() > 0){
				if(socket_excutor == null){
					socket_excutor = Executors.newSingleThreadExecutor();
				}
				
				String curr_id = wait_list.remove(0);
				socket_excutor.execute(new run_request(request_info_map.remove(curr_id)));
			}
			
		}
	}
	
	private class run_request implements Runnable{
		
		String farm;
		String dong;
		String request_date;
		
		JSONObject json;
		ByteBuffer buffer;
		Charset charset = Charset.forName("UTF-8");
		
		JSONParser json_parser = new JSONParser();
		
		@SuppressWarnings("unchecked")
		public run_request(HashMap<String, String> m_map){
			
			farm = m_map.get("rcFarmid");
			dong = m_map.get("rcDongid");
			request_date = m_map.get("rcRequestDate");
			
			//json = new JSONObject(m_map);
			json = new JSONObject();
			json.put("command", "recalc");
			json.put("data", new JSONObject(m_map));
		}
		
		@SuppressWarnings("unchecked")
		public void run(){
			
			//{"rcFarmid":"KF0011", "rcDongid":"01", "rcRequestDate":"...", "rcCode":"...", "rcCommand":"Lst|Day|Opt", "rcChangeLst":"삼계", "rcChangeDate":"...", "rcMeasureDate":"...", "rcMeasureVal",:"200.0"}
			
			// 산출 시작 후 C(calculate) 상태로 변환
			change_request_status(farm, dong, request_date, "C");
			
			try {
				FarmInfo.list_map.get(farm + dong).set_is_recalc(true);
			} catch (NullPointerException  e) {
				// TODO Auto-generated catch block
				FileUtil.write("ERROR => NullPointerException in set_is_recalc / Class : request_worker");
				change_request_status(farm, dong, request_date, "I");
				return;
			}
			
			String read = "";
			
			String trim_date = DateUtil.get_inst().get_plus_minus_minute_time(DateUtil.get_inst().get_now().substring(0, 15) + "0:00", -10);
			((JSONObject)json.get("data")).put("trim_date", trim_date);
			
			try {
				SocketChannel socket;
				socket = SocketChannel.open();
				socket.configureBlocking(true);
				socket.connect(new InetSocketAddress("localhost", 4000));
				
				String send = JSONValue.toJSONString(json);
				buffer = charset.encode(send);
				socket.write(buffer);
				
				//JSON 데이터를 string 형식으로 수신
				buffer = ByteBuffer.allocate(1024);
				socket.read(buffer);
				buffer.flip();
				
				// 읽어온 데이터를 저장 후 소켓 종료
				read = charset.decode(buffer).toString().trim();
				
				buffer.clear();
				
				if(socket != null && socket.isOpen()) {
					socket.close();
					socket = null;
				}
				
			} catch (IOException e) {
				e.printStackTrace();
				FileUtil.write("ERROR => IOException in run_request / Class : request_worker");
			}
			
			// 대기열 비움
			curr_id = "";
			
			if(recalculate_excutor == null){
				recalculate_excutor = Executors.newSingleThreadExecutor();
			}
			
			// 받아온 결과에 따라 마지막 작업 처리
			try {
				
				if(read.equals("")){
					read = "{\"result\":\"ERROR\"}";
				}
				
				JSONObject read_jo = (JSONObject) json_parser.parse(read);
				
				switch ((String) read_jo.get("result")) {
				case "OK":
					// 산출 종료 후 작업
					// 재산출된 히스토그램 폴더를 복사하여 가져옴
					String main_path = "/home/kokofarm/KKF_EWAS/DATA/" + farm + "/" + dong;
					String sub_path = "/home/kokofarm/KKF_EWAS_SUB/DATA/" + farm + "/" + dong;
					FileUtil.file_delete(main_path);
					FileUtil.file_copy(sub_path, main_path);
					
					// 그 후 처리는 recalculate_avg에 위임
					recalculate_excutor.execute(new RecalcAvg(farm, dong, trim_date, DateUtil.get_inst().get_now(), request_date, true));
					break;

				default :
					// 산출 오류시 I (Interrupt) 상태로 변환
					change_request_status(farm, dong, request_date, "I");
					recalculate_excutor.execute(new RecalcAvg(farm, dong, trim_date, DateUtil.get_inst().get_now(), request_date, false));
					break;
				}
				
			} catch (ParseException e) {
				e.printStackTrace();
				FileUtil.write("ERROR => ParseException in read_jo / Class : request_worker");
			} catch (Exception e){
				e.printStackTrace();
				FileUtil.write("ERROR => Exception in read_jo / Class : request_worker");
			}
		}
	}
}
