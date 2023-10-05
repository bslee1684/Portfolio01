package module;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import util.FileUtil;

public class AvgCalcBridge {
	
	Charset charset;
	JSONParser json_parser;
	int avg_port = (int) (long) FileUtil.get_config("avg_port");
	
	// 정상 평체 산출 로직에서 사용중인지 확인
	public boolean is_orgin_run = false;
	public String last_origin_time = "2001-01-01 00:00:00";
	
	private static AvgCalcBridge inst = null;
	
	private AvgCalcBridge(){
		charset = Charset.forName("UTF-8");
		json_parser = new JSONParser();
	}
	
	public static AvgCalcBridge get_inst(){
		if(inst == null){
			inst = new AvgCalcBridge();
		}
		
		return inst;
	}
	
	private String call_algorithm(String send){
		
		ByteBuffer buffer = null;
		String read = "";
		
		try {
			SocketChannel socket = SocketChannel.open();
			socket.configureBlocking(true);
			socket.connect(new InetSocketAddress("localhost", avg_port));
			
			//JSON 데이터를 만든 후 string 형식으로 송신
			buffer = charset.encode(send);
			socket.write(buffer);
			
			//JSON 데이터를 string 형식으로 수신
			buffer = ByteBuffer.allocate(1024);
			socket.read(buffer);
			buffer.flip();
			
			read = charset.decode(buffer).toString().trim();
			read = read.replace("NaN", "0.0");
			
			buffer.clear();
			
			if(socket != null && socket.isOpen()) {
				socket.close();
				socket = null;
			}
			
		} catch (IOException e) {
			read = "error";
			FileUtil.write("ERROR => IOException Avg Return Error in call_avg_calc");
		}
		
		return read;
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject get_avg_calc(String send, String get_time){
		
		JSONObject result = null;
		
		try {
			String read = call_algorithm(send);
			
			if(read.equals("error")){				// 호출 중 오류 발생한 경우
				result = new JSONObject();
				result.put("success", "error");
			}
			else{
				//받은 string 데이터를 JSON 객체로 변환
				result = (JSONObject) json_parser.parse(read);
				result.put("success", "ok");
				
				if(((String) result.get("farmID")).equals("")) {		//평균중량 알고리즘에서 에러를 리턴받은 경우 처리
					result.clear();
					result.put("success", "error");
				}
				
				//2021-01-29 이병선 수정 육계종계를 처리하지 못하여 임시적으로 에러조치
				if(((String) result.get("getTime")).equals("")){
					result.put("getTime", get_time);
				}
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			FileUtil.write("ERROR => ParseException Avg Return Error in avg_call");
			result = new JSONObject();
			result.put("success", "error");
		} 
		
		return result;
	}
	
	// 파이썬 새로 개발된 알고리즘 데이터 초기화 명령
	public JSONObject send_init(String farm, String dong, String type, String in_time, int day){
		String send = make_init_json(farm, dong, type, in_time, day);		//초기화 json 형식
		
		return get_avg_calc(send, in_time);
	}
	
	// 2021-02-23 평체 산출 초기화 명령 및 동별 top bot 적용
	public JSONObject send_empty(String farm, String dong, String type, String in_time, int day){
		String send = make_empty_avg_json(farm, dong, type, in_time, day);		//초기화 json 형식
		
		return get_avg_calc(send, in_time);
	}
	
	// 빈 배열을 넣은 알고리즘 호출 json 만들기 - 초기화 및 데이터 없는 경우 위함
	public String make_empty_avg_json(String farm, String dong, String type, String in_time, int day){
		List<Document> init_doc = new ArrayList<Document>();
		List<Integer> arr = new ArrayList<Integer>();
		arr.add(0);
		init_doc.add(new Document().append("jeoulID", "01").append("data", arr));
		
		String send = make_avg_json( init_doc, farm, dong, type, in_time, day);		//초기화 json 형식
		
		return send;
	}
	
	//평균중량 산출 알고리즘 호출을 위한 json 데이터 메소드 (개별 저울 방식)
	@SuppressWarnings("unchecked")
	public String make_avg_json(List<Document> data_list, String m_farmID, String m_dongID, String m_type, String m_Time, int m_Days) {
		String avg_send_json = "";
			
		switch (m_type) {
		case "육계":
			m_type = "A";
			break;
		case "삼계":
			m_type = "B";
			break;
		case "토종닭":
			m_type = "C";
			break;
		case "산란계":
			m_type = "D";
			m_Days = day_to_week(m_Days);
			break;
		case "육계종계":
			m_type = "E";
			//일령을 주령으로 변환
			m_Days = day_to_week(m_Days);
			break;
		case "산란계종계":
			m_type = "F";
			//일령을 주령으로 변환
			m_Days = day_to_week(m_Days);
			break;
		default:
			m_type = "A";
			break;
		}
			
		JSONObject data_jo = new JSONObject();
		
		for(Document cell : data_list) {
			
			// 211101 이병선 다부처 추가 - 상층부 저울은 사용하지 않음
			String jeoulID = cell.getString("jeoulID");
			int test = Integer.parseInt(jeoulID);
			if(test >= 13) {
				continue;
			}
			
			String cell_id = "jeoul" + cell.getString("jeoulID");
			List<Integer> cell_data = (List<Integer>) cell.get("data");
			
			//로우데이터가 모두 0일 경우 38로 강제 조정
			if(m_Days == 1 && !m_Time.substring(11).equals("00:00:00")){
				int sum = 0;
				for(int val : cell_data){
					sum += val;
				}
				
				if(sum <= 0){
					int len = cell_data.size();
					cell_data.clear();
					for(int i=0; i<len; i++){
						cell_data.add(i, 38);
					}
				}
			}
			
			//211101 이병선 다부처 추가 - 오류 저울 값 제거
			Iterator<Integer> it = cell_data.iterator();
			while (it.hasNext()) {
				int item = it.next();
				
				if(item == -200) {
					it.remove();
				}
			}
			
			data_jo.put(cell_id, cell_data);
		}
			
		JSONObject jo = new JSONObject();
		jo.put("Command", "MDATA");
		jo.put("farmID", m_farmID);
		jo.put("dongID", m_dongID);
		jo.put("Type", m_type);
		jo.put("Time", m_Time);
		jo.put("Days", m_Days);
		//jo.put("Count", data_list.size());
		jo.put("Data", data_jo);
		
		avg_send_json = JSONValue.toJSONString(jo);
			
		return avg_send_json  + "\n\0";
	}
	
	//평균중량 산출 알고리즘 호출을 위한 json 데이터 메소드 (개별 저울 방식)
	@SuppressWarnings("unchecked")
	public String make_init_json(String m_farmID, String m_dongID, String m_type, String m_Time, int m_Days) {
		
		String avg_send_json = "";
			
		switch (m_type) {
		
		default:
			m_type = "A";
			break;
		}
			
		JSONObject data_jo = new JSONObject();
		List<Integer> cell_data = new ArrayList<Integer>();
		cell_data.add(0);
		data_jo.put("jeoul01", cell_data);
			
		JSONObject jo = new JSONObject();
		jo.put("Command", "INIT");
		jo.put("farmID", m_farmID);
		jo.put("dongID", m_dongID);
		jo.put("Type", m_type);
		jo.put("Time", m_Time);
		jo.put("Days", m_Days);
		jo.put("Data", data_jo);
		
		avg_send_json = JSONValue.toJSONString(jo);
			
		return avg_send_json  + "\n\0";
	}
	
	//일령 => 주령 변환 //종계용
	public strictfp int day_to_week(int day){
		int week = 0;
		week = day-1;
		week = (week/7) + 1;
		return week;
	}

}
