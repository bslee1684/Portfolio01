package p3300;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.json.simple.JSONObject;

import kokofarm.ExternSensor;
import kokofarm.FarmInfo;
import kokofarm.LightSensor;
import kokofarm.SensorUnit;
import server.ServerWorker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;

public class P3300Worker extends ServerWorker {
	
	//------------------------------------------------------
	// 기본 프로토콜
	// START  TARGET  COMM  LEN  DATA  CRC  END
	//   AA                                  EE
	//------------------------------------------------------
	
	private static final byte[] ACK_TO_GW_PACKET = new byte[] {(byte) 0xAA, (byte) 0x85, (byte) 0x90, (byte) 0x00, (byte) 0x15, (byte) 0xEE };
	private static final String[] CELL_PAIR_ARR = new String[] {"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};// 상하층 페어 배열
	
	private FarmInfo info = null;
	private String farm = "KF0000";
	private String dong = "00";
	
	// 현재 펌웨어 정보 / 업데이트를 위한 펌웨어 파일 정보
	private String[] firmware_data;

	public P3300Worker(Socket m_socket) {
		super(m_socket);
	}
	
	// 패킷 수신시 작업을 구현
	@Override
	protected void receive_work(byte[] packet) {
		
		//FileUtil.write("RECV => " + FileUtil.byte_to_string(packet));
		
		switch (get_byte_idx(packet, 2)) {
		
			case (byte) 0x61: // 최초 소켓 연결
				String farmID = String.format("KF%04d", ((get_byte_idx(packet, 4) & 0xFF) << 8) | (get_byte_idx(packet, 5) & 0xFF));
				String dongID = String.format("%02d", get_byte_idx(packet, 6) & 0xFF);
				FileUtil.write("COMPLETE => Farm Info Receive " + farmID + " " + dongID);
				put_socket_map(farmID, dongID);
				
				info = FarmInfo.get_inst(farmID, dongID);
				
				packet = swap_byte_idx(packet, 1, (byte) 0x85);
				packet = swap_byte_idx(packet, 2, (byte) 0x91);
				packet = swap_byte_idx(packet, packet.length - 2, make_crc(packet));
				send(packet); // ACK 송신
				break;
		
			case (byte) 0x60: // 연결상태조회
				send(ACK_TO_GW_PACKET); // ACK 송신
				break;
	
			case (byte) 0x63: // //현재시간 조회
				send(make_time_packet());
				break;
				
			case (byte) 0x65: // 데이터 전송 완료 패킷
				InetAddress inet_address = socket.getInetAddress();	//sparrow 수정사항
				if(inet_address != null) {
					info.set_ip_addr(inet_address.toString().replace("/", ""));
				}
				info.work();		// db 저장 로직 실행
				break;
				
			case (byte) 0x4C:		// 브릿지 모듈 데이터
				send(ACK_TO_GW_PACKET); // ACK 송신
				add_ext_data(packet);
				break;
	
			case (byte) 0x50: // 무게데이터(신형)
				send(ACK_TO_GW_PACKET); // ACK 송신
				try {
					add_cell_data(packet, socket.getInetAddress().toString());
				} catch (IndexOutOfBoundsException e) {
					FileUtil.write("ERROR => IndexOutOfBoundsException in add_cell_data / packet : " + FileUtil.byte_to_string(packet));
				}
				
				break;
				
			case (byte) 0x51: // 조도 데이터
				send(ACK_TO_GW_PACKET); // ACK 송신
				add_light_data(packet);
				
				break;
	
			case (byte) 0x53: // 원격 제어기설정명령
				packet = swap_byte_idx(packet, 2, (byte) 0x4E); // 제어기 설정명령으로 변환
				send_order(packet);
				break;
	
			case (byte) 0x54: // 원격 저울설정명령
				packet = swap_byte_idx(packet, 2, (byte) 0x4F);
				send_order(packet);
				break;
	
			case (byte) 0x6D: // H/W조회명령
				packet = swap_byte_idx(packet, 2, (byte) 0x6C);
				send_order(packet);
				break;
				
			case (byte) 0x90:
				// ack 응답 (현재 받을일 없음)
				break;
	
			case (byte) 0x94:
				if (get_byte_idx(packet, 3) == (byte) 0x05) { // 펌웨어 업데이트
					send(make_firmware_packet(packet));
				}
				break;
			
			case (byte) 0x9C: // 6C (조회 명령 응답)
				pass_to_waiter(packet);
				break;
				
			case (byte) 0x9E: // 4E (GW 제어명령 응답)
				pass_to_waiter(packet);
				break;
				
			case (byte) 0x9F: // 4F (저울 제어명령 응답)
				pass_to_waiter(packet);
				break;
		
		} // switch
	}
	
	@Override
	protected boolean send(byte[] packet){
		//FileUtil.write("SEND => " + FileUtil.byte_to_string(packet));
		return super.send(packet);
	}
	
	private byte get_byte_idx(byte[] bytes, int idx) {
		byte ret = (byte) 0x00;
		
		if(idx > 0 && bytes.length > idx) {
			ret = bytes[idx];
		}
		
		return ret;
	}
	
	private byte[] swap_byte_idx(byte[] bytes, int idx, byte swap) {
		if(idx > 0 && bytes.length > idx) {
			bytes[idx] = swap;
		}
		return bytes;
	}
	
	private void put_socket_map(String farmID, String dongID){
		id = farmID + dongID;
		
		farm = farmID;
		dong = dongID;
		
		P3300Listner.socket_map.put(id, this.socket);
	}
	
	private byte[] make_time_packet(){
		byte[] ret = new byte[12];
		
		String now = DateUtil.get_inst().get_now_simple();
		
		ret[0] = byte_head;
		ret[1] = byte_send_target;
		ret[2] = (byte) 0x93;		// 현재시간 조회 응답
		ret[3] = (byte) 0x06;
		ret[4] = (byte) (Integer.parseInt(now.substring(2, 4)) & 0xFF); // year
		ret[5] = (byte) (Integer.parseInt(now.substring(4, 6)) & 0xFF); // month
		ret[6] = (byte) (Integer.parseInt(now.substring(6, 8)) & 0xFF); // day
		ret[7] = (byte) (Integer.parseInt(now.substring(8, 10)) & 0xFF); // hour
		ret[8] = (byte) (Integer.parseInt(now.substring(10, 12)) & 0xFF); // min
		ret[9] = (byte) (Integer.parseInt(now.substring(12, 14)) & 0xFF); // sec
		ret[10] = (byte) 0x00;
		ret[11] = byte_tail;
		
		ret[ret.length - 2] = make_crc(ret);
		
		return ret;
	}
	
	// 펌웨어 데이터 전송 패킷 (원격 업데이트)
	private byte[] make_firmware_packet(byte[] recv_packet){
		
		byte[] ret = null;
		
		int idx = ((recv_packet[7] & 0xFF) << 8) | (recv_packet[8] & 0xFF);
		
		if(idx == 0){
			read_firmware();
			
			FileUtil.write("Start => Firmware Update Start " + id);
		}
		
		if(idx < firmware_data.length){
			String data = firmware_data[idx].replace("\r", "");
			
			try {
				ret = FileUtil.ascii_to_byte(data);
			}
			catch(StringIndexOutOfBoundsException e) {
				FileUtil.write("ERROR => StringIndexOutOfBoundsException Class : P3300Worker / Function : make_firmware_packet");
			}
			catch(Exception e) {
				FileUtil.write("ERROR => Exception Class : P3300Worker / Function : make_firmware_packet");
			}
		}
		else{
			FileUtil.write("End => Firmware Update End " + id);
			firmware_data = null;
		}
		
		return ret;
		
	}
	
	//펌웨어 데이터를 읽어오는 메소드
	private void read_firmware() {
		String firmware_path = "./firmware/gw.hex";
		firmware_data = FileUtil.file_read(firmware_path).split("\\n");
//		firmware_data = Arrays.asList(data);
	}
	
	// PHP에서 원격명령 전송 시 연결된 농장의 소켓을 찾아 전달함
	@SuppressWarnings("unchecked")
	private void send_order(byte[] packet){
		String target_farm = String.format("KF%04d", ((packet[4] & 0xFF) << 8) | (packet[5] & 0xFF));
		String target_dong = String.format("%02d", packet[6] & 0xFF);
		
		String key = target_farm + target_dong;
		
		try {
			if(P3300Listner.socket_map.containsKey(key)) {		//현재 접속된 통합G/W 리스트에 명령 대상이 존재하면 실행
				Socket target_socket = P3300Listner.socket_map.get(key);
				
				if(target_socket.isConnected()) {
					DataOutputStream target_dos = new DataOutputStream(target_socket.getOutputStream());
					
					P3300Listner.waiter_map.put(key, this.socket);
					
					target_dos.write(make_order_packet(packet));
					target_dos.flush();
					
					FileUtil.write("COMPLETE => Send to " + id + " COMM : 0x" + String.format("%02X", packet[2] & 0xff));
					
					return;
				}
				else{
					FileUtil.write("ERROR => Exception " + id + " is not connect");
				}
			}
			else{
				FileUtil.write("ERROR => Exception " + id + " is not connect");
			}
			
		}catch(IOException e) {
			FileUtil.write("ERROR => IOException " + id + " send_order Error");
		}catch(Exception e) {
			FileUtil.write("ERROR => Exception " + id + " send_order Error");
		} 
		
		JSONObject jo = new JSONObject();
		
		try {
			//명령 전송 실패 메시지를 php, c#에 전달
			jo.put("retFarm", target_farm);
			jo.put("retDong", target_dong);
			jo.put("retCell", "00");
			jo.put("retType", "");
			jo.put("retValue", "");
			jo.put("retCode", "F");
			
			dos.writeUTF(jo.toJSONString());
		} catch (IOException e) {
			FileUtil.write("ERROR => IOException " + id + " send_order writeUTF Error");
		}
	}
	
	//PHP에서 서버로 전달된 명령을 통합 G/W 명령으로 변환하는 함수
	private byte[] make_order_packet(byte[] packet) throws Exception {
		
		byte[] ret = new byte[packet.length - 3];

		ret[0] = byte_head;
		ret[1] = byte_send_target;
		ret[2] = packet[2];
		ret[3] = (byte) (packet[3] - 3); // length

		for(int i = 4; i < ret.length - 2; i++) {
			ret[i] = packet[i + 3];
		}

		ret[ret.length - 2] = make_crc(ret);
		ret[ret.length - 1] = byte_tail;
		
		FileUtil.write("TEST => " + FileUtil.byte_to_string(ret));

		return ret;
	}
	
	//농장에서 전달 받은 원격 명령 응답을 PHP 소켓을 찾아서 전달
	@SuppressWarnings("unchecked")
	private void pass_to_waiter(byte[] packet) {
		JSONObject jo = new JSONObject();
		
		jo.put("retFarm", farm);
		jo.put("retDong", dong);
		
		int data_len = (int)packet[3] & 0xFF;		//DATA BYTE 길이
		
		StringBuilder data_builder = new StringBuilder();
		
		for(int i=0; i<data_len; i++){
			data_builder.append((char)packet[4+i]);
		}
		
		String data = data_builder.toString();
		
		FileUtil.write("TEST => " + data);
		
		try {
			String[] tokens = data.split("-");
			
			if(tokens.length == 2){
				String val = tokens[1];
				
				jo.put("retType", tokens[0].substring(0, 1));
				jo.put("retCell", tokens[0].substring(1));
				jo.put("retCode", val.equals("F") ? "F" : "S");
				jo.put("retValue", val);
			}
			else{
				jo.put("retType", "");
				jo.put("retCell", "");
				jo.put("retCode", "F");
				jo.put("retValue", "F");
			}
		} catch (IndexOutOfBoundsException e1) {
			e1.printStackTrace();
		}
		
		try {
			Socket waiter = P3300Listner.waiter_map.get(id);
			
			FileUtil.write("TEST => " + jo.toJSONString());
			
			DataOutputStream waiter_dos = new DataOutputStream(waiter.getOutputStream());
			waiter_dos.writeUTF(jo.toJSONString());
			waiter_dos.flush();
			P3300Listner.waiter_map.remove(id);
			
		}catch(IOException e) {
			FileUtil.write("ERROR => Pass to Client Error in pass_to_waiter()");
		}catch(Exception e) {
			FileUtil.write("ERROR => Pass to Client Error in pass_to_waiter()");
		}
	}
	
	//----------------------------------------------------------------------------------------
	// packet		입력스트림에서 받은 데이터
	// ip_addr		연결된 소켓의 IP주소	
	//----------------------------------------------------------------------------------------
	public strictfp void add_cell_data(byte[] packet, String ip_addr) throws IndexOutOfBoundsException{
		
		int idx = 4;
		
		//환경데이터 및 시간값 파싱
		double temp = 0.0, humi = 0.0, co = 0.0, nh = 0.0, dust_high = 0.0, dust_low = 0.0;
		String farmID = String.format("KF%s", String.format("%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF)));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		String jeoulID = String.format("%02d", packet[idx++] & 0xFF);
		String year = String.format("%02d", packet[idx++] & 0xFF);
		String month = String.format("%02d", packet[idx++] & 0xFF);
		String date = String.format("%02d", packet[idx++] & 0xFF);
		String hour = String.format("%02d", packet[idx++] & 0xFF);
		String min = String.format("%02d", packet[idx++] & 0xFF);
		String sec = String.format("%02d", packet[idx++] & 0xFF);
		String getTime = 20 + year + "-" + month + "-" + date + " " + hour + ":" + min + ":" + sec;
		
		put_socket_map(farmID, dongID);
		
		temp = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		humi = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		co = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		nh = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		dust_high = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		dust_low = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		
		temp = check_err_data((int) temp, 10.0);
		humi = check_err_data((int) humi, 10.0);
		nh = check_err_data((int) nh, 10.0);
		
		dust_high = check_err_data((int) dust_high, 1.0);
		dust_low = check_err_data((int) dust_low, 1.0);

		int max_weight = 0;

		int[] w = new int[60];
		int t = 0;

		// idx 27부터 시작
		if (packet.length > idx + 2) { // 무게 데이터 파싱
			for (int i = idx; i < packet.length - 2; i += 2) {	//무게 데이터 값이 존재하는지 확인 후 있을 경우 순차적으로 파싱
				int weight = (((packet[i] & 0xFF) << 8) | (packet[i + 1] & 0xFF));	//무게 데이터는 2byte
				weight = check_err_data(weight);
				
				if (weight > max_weight) {	//무게 데이터의 최대값 구하기
					max_weight = weight;
				}

				try {
					w[t++] = weight;
				} catch (IndexOutOfBoundsException e) {
					FileUtil.write("ERROR => IndexOutOfBoundsException in add_cell_data");
				}
			}
		}
		
		//------------------------------------------------------------------------------------
		//몽고db 데이터 적재
		//------------------------------------------------------------------------------------
		SensorUnit unit = info.get_cell(jeoulID);
		unit.set_val("_id", farmID + dongID + jeoulID + "_" + DateUtil.get_inst().get_timestamp(year, month, date, hour, min, sec));
		unit.set_val("farmID", farmID);
		unit.set_val("dongID", dongID);
		unit.set_val("jeoulID", jeoulID);
		unit.set_val("getTime", getTime);
		unit.set_val("temp", temp);
		unit.set_val("humi", humi);
		unit.set_val("co", co);
		unit.set_val("nh", nh);
		unit.set_val("dust", dust_high);
		
		// 페어를 맞춰서 하층부 미세먼지는 하층센서 번호로 넣어줌
		String pair_num = CELL_PAIR_ARR[Integer.parseInt(jeoulID) - 1];
		if(!pair_num.equals("0")) {
			info.get_cell(pair_num).set_val("dust", dust_low);;
		}

		for (int z = 0; z < 60; z++) {
			unit.set_val("w" + String.format("%02d", z + 1), w[z]);
		}
		
		unit.set_max_weight(max_weight);
		
		info.add_sensor_data_list(unit.get_doc());		// 몽고db 업데이트 대기열에 적재
		info.set_last_sensor_date(getTime); 			// 최종 수집시간
		
		//FileUtil.write("CELL => " + unit.get_doc().toJson());
	}
	
	//급이, 급수, 외기환경 센서 데이터 적재
	private strictfp void add_ext_data(byte[] packet) {
		
		int idx = 4;
		
		//데이터 초기화 - 외기환경 - 없을 경우 0으로 적재
		double temp = 0.0, humi = 0.0, nh3 = 0.0, h2s = 0.0, w_speed = 0.0;
		int pm10 = 0, pm25 = 0, w_direction = 0, solar = 0;
		
		// -- idx 4부터
		String farmID = String.format("KF%s", String.format("%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF)));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		String year = String.format("%02d", packet[idx++] & 0xFF);
		String month = String.format("%02d", packet[idx++] & 0xFF);
		String date = String.format("%02d", packet[idx++] & 0xFF);
		String hour = String.format("%02d", packet[idx++] & 0xFF);
		String min = String.format("%02d", packet[idx++] & 0xFF);
		String sec = String.format("%02d", packet[idx++] & 0xFF);
		String getTime = 20 + year + "-" + month + "-" + date + " " + hour + ":" + min + ":" + sec;
		
		// -- idx 13부터
		int feed_weight = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		int feed_water = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		
		feed_weight = check_err_data(feed_weight);
		feed_water = check_err_data(feed_water);
		
		// -- idx 17부터
		boolean has_ext = false;
		//외기환경 존재할 경우 파싱
		if(packet.length > idx + 2) {
			has_ext = true;
			
			temp = get_code_data(packet[idx++], packet[idx++]);							//외기 온도
			humi = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//외기 습도
			
			nh3 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//암모니아
			h2s = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//황화수소
			pm10 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//미세먼지
			pm25 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//초미세먼지
			
			w_speed = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));				//풍속
			w_direction = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));			//풍향
			
			solar = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));					//일사량
			
			temp = check_err_data((int) temp, 10.0);
			humi = check_err_data((int) humi, 10.0);
			nh3 = check_err_data((int) nh3, 10.0);
			h2s = check_err_data((int) h2s, 10.0);
			w_speed = check_err_data((int) w_speed, 10.0);
			
			pm10 = check_err_data(pm10);
			pm25 = check_err_data(pm25);
			w_direction = check_err_data(w_direction);
			solar = check_err_data(solar);
		}
		
		ExternSensor ext = info.get_extern_sensor();
		ext.set_val("_id", farmID + dongID + "_" + DateUtil.get_inst().get_timestamp(year, month, date, hour, min, sec));
		ext.set_val("farmID", farmID);
		ext.set_val("dongID", dongID);
		ext.set_val("getTime", getTime);
		ext.set_val("feedWeight", feed_weight);
		ext.set_val("feedWeightval", 0);
		ext.set_val("feedWater", feed_water);
		ext.set_val("feedWaterval", 0);
		ext.set_val("outTemp", temp);             
		ext.set_val("outHumi", humi);             
		ext.set_val("outNh3", nh3);               
		ext.set_val("outH2s", h2s);               
		ext.set_val("outDust", pm10);             
		ext.set_val("outUDust", pm25);            
		ext.set_val("outWinderec", w_direction);  
		ext.set_val("outWinspeed", w_speed);      
		ext.set_val("outSolar", solar);    
		
		ext.update(has_ext);
		
		//FileUtil.write("EXT => " + ext.get_doc().toJson());
	}
	
	//급이, 급수, 외기환경 센서 데이터 적재
	private strictfp void add_light_data(byte[] packet) {
		
		int idx = 4;
		
		//데이터 초기화 - 외기환경 - 없을 경우 0으로 적재
		int light1 = 0, light2 = 0, light3 = 0, light4 = 0;
		
		// -- idx 4부터
		String farmID = String.format("KF%s", String.format("%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF)));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		String year = String.format("%02d", packet[idx++] & 0xFF);
		String month = String.format("%02d", packet[idx++] & 0xFF);
		String date = String.format("%02d", packet[idx++] & 0xFF);
		String hour = String.format("%02d", packet[idx++] & 0xFF);
		String min = String.format("%02d", packet[idx++] & 0xFF);
		String sec = String.format("%02d", packet[idx++] & 0xFF);
		String getTime = 20 + year + "-" + month + "-" + date + " " + hour + ":" + min + ":" + sec;
		
		// -- idx 13부터
		light1 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		light2 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		light3 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		light4 = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		
		light1 = check_err_data(light1);
		light2 = check_err_data(light2);
		light3 = check_err_data(light3);
		light4 = check_err_data(light4);
		
		LightSensor ls = info.get_light_sensor();
		
		ls.set_val("_id", farmID + dongID + "_" + DateUtil.get_inst().get_timestamp(year, month, date, hour, min, sec));
		ls.set_val("farmID", farmID);
		ls.set_val("dongID", dongID);
		ls.set_val("getTime", getTime);
		ls.set_val("light01", light1);
		ls.set_val("light02", light2);
		ls.set_val("light03", light3);
		ls.set_val("light04", light4);
		
		ls.update();
		
		//FileUtil.write("LIGHT => " + ls.get_doc().toJson());
	}
	
	private int get_code_data(byte byte1, byte byte2) {
//		int a = (byte1 & 0x7F);
//		int b = (byte2 & 0xFF);
		
		int ret = ((byte1 & 0x7F) << 8) | (byte2 & 0xFF);
		ret = (byte1 & 0xFF) > 127 ? ret * -1 : ret;
		
		return ret;
	}
	
	private int check_err_data(int val) {
		
		int ret = val;
		
		switch(val) {
		
		case 65535:		// 단선 E1
			ret = -100;
			break;
			
		case 65534:		// 단락 E2
			ret = -200;
			break;
		}
		
		return ret;
	}
	
	private double check_err_data(int val, double div) {
		
		double ret = (double) val;
		
		switch(val) {
		
		case -32767:
		case 65535:		// 단선 E1
			ret = (double) -100;
			break;
			
		case -32766:
		case 65534:		// 단락 E2
			ret = (double) -200;
			break;
			
		default:
			ret = FloatCompute.divide(val, div);
			break;
		}
		
		return ret;
	}
}
