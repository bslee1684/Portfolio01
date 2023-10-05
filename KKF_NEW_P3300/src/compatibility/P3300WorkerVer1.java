package compatibility;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.json.simple.JSONObject;

import kokofarm.ExternSensor;
import kokofarm.SensorUnit;
import p3300.P3300Listener;
import p3300.P3300Worker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.MongoConn;

public class P3300WorkerVer1 extends P3300Worker{
	
	static final byte[] FARM_CHECK_PACKET = new byte[] {(byte) 0xAA, (byte) 0x85, (byte) 0x6C, (byte) 0x01, (byte) 0x46, (byte) 0xAE, (byte) 0xEE};

	public P3300WorkerVer1(Socket m_socket, boolean commander) {
		super(m_socket);
	}
	
	public P3300WorkerVer1(Socket m_socket) {
		super(m_socket);
		//send(ACK_TO_GW_PACKET); // ACK 송신
		send(FARM_CHECK_PACKET); // 농장 정보 조회
	}
	
	@Override
	protected void receive_work(int count, byte[] input) {
		
		//FileUtil.write("RECV => " + farm + " " + dong + " : " + FileUtil.byte_to_string(packet));
		
		// 수신된 모든 바이트를 버퍼에 담음
		for(int i=0; i<count; i++) {
			receive_buffer.offer(input[i]);
		}
		
		//Byte 버퍼에 데이터가 남아 있으면 계속 진행 함 -> 패킷 2개 이상 존재 시 모두 수행한 후 다음 데이터를 받기 위함
		while(!receive_buffer.isEmpty()) {
			
			//ByteBuffer에서 데이터를 packetList에 하나씩 적재하면서 패킷 끝 부분을 찾음
			if(check_packet()) {
				byte[] packet = new byte[packet_maker.size()];	//패킷이 완성되면 바이트 배열로 옮긴 후 packetList를 클리어
				for(int j=0; j<packet_maker.size(); j++) {
					packet[j] = packet_maker.get(j);
				}
				packet_maker.clear();
		
				switch (get_byte_idx(packet, 2)) {
				
					case (byte) 0x60: // 연결상태조회
						send(ACK_TO_GW_PACKET); // ACK 송신
						break;
			
					case (byte) 0x63: // //현재시간 조회
						send(make_time_packet());
						break;
						
					case (byte) 0x4C:		// 브릿지 모듈 데이터
						send(ACK_TO_GW_PACKET); // ACK 송신
						add_ext_data(packet);
						break;
			
					case (byte) 0x50: // 무게데이터(신형)
						send(ACK_TO_GW_PACKET); // ACK 송신
						try {
							add_cell_data(packet, socket.getInetAddress().toString().replace("/", ""));
						} catch (IndexOutOfBoundsException e) {
							FileUtil.write("ERROR => IndexOutOfBoundsException in add_cell_data / packet : " + FileUtil.byte_to_string(packet));
						}
						
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
						if (get_byte_idx(packet, 3) == (byte) 0x05) { // 펌웨어 업데이트
							send(make_firmware_packet(packet));
						}
						break;
					
					case (byte) 0x9C: // 6C (조회 명령 응답)
						if (get_byte_idx(packet, 3) == (byte) 0x03) { // 농장, 동 조회 명령 인 경우
							String farmID = String.format("KF%04d", ((get_byte_idx(packet, 4) & 0xFF) << 8) | (get_byte_idx(packet, 5) & 0xFF));
							String dongID = String.format("%02d", get_byte_idx(packet, 6) & 0xFF);
							FileUtil.write("COMPLETE => Farm Info Receive " + farmID + " " + dongID);
							put_socket_map(farmID, dongID);
							
							// 농장 정보 관리 인스턴스에 id를 세팅함
							info = FarmInfoVer1.get_inst(farmID, dongID);
							
						} else { // 그 외 설정, 조회 명령인 경우 waiter로 전달
							pass_to_waiter(packet);
						}
						break;
						
					case (byte) 0x9E: // 4E (GW 제어명령 응답)
						pass_to_waiter(packet);
						break;
						
					case (byte) 0x9F: // 4F (저울 제어명령 응답)
						pass_to_waiter(packet);
						break;
				
				} // switch
		
			}	//check_packet
		} //while(!ByteBuffer.isEmpty())
	}
	
	@Override
	public strictfp void add_cell_data(byte[] packet, String ip_addr) throws IndexOutOfBoundsException{
		
		int idx = 4;
		
		//환경데이터 및 시간값 파싱
		double temp = 0.0, temp1 = 0.0, temp2 = 0.0, humi = 0.0, humi1 = 0.0, humi2 = 0.0, co = 0.0, nh = 0.0, nh1 = 0.0, nh2 = 0.0;
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
		
		info = FarmInfoVer1.get_inst(farmID, dongID);
		
//		temp = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
//		humi = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
//		co = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
//		nh = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		
		temp1 = packet[idx++] & 0xFF;
		temp2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
		humi1 = packet[idx++] & 0xFF;
		humi2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
		co = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		nh1 = packet[idx++] & 0xFF;
		nh2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
		
		temp = temp1 + temp2;
		humi = humi1 + humi2;
		nh = nh1 + nh2;
		
		// 20220321 온습도 보정 요청 사항 적용 +7%
		temp = temp - 1.2;
		humi = humi + 7.0;
		humi = humi > 99.0 ? 99.0 : humi;

		int max_weight = 0;

		int[] w = new int[60];
		int t = 0;

		// idx 27부터 시작
		if (packet.length > idx + 2) { // 무게 데이터 파싱
			for (int i = idx; i < packet.length - 2; i += 2) {	//무게 데이터 값이 존재하는지 확인 후 있을 경우 순차적으로 파싱
				int weight = (((packet[i] & 0xFF) << 8) | (packet[i + 1] & 0xFF));	//무게 데이터는 2byte
				
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

		for (int z = 0; z < 60; z++) {
			unit.set_val("w" + String.format("%02d", z + 1), w[z]);
		}
		
		unit.set_max_weight(max_weight);
		
		//info.add_sensor_data_list(unit.get_doc());		// 몽고db 업데이트 대기열에 적재
		info.set_last_sensor_date(getTime); 			// 최종 수집시간
		info.set_ip_addr(ip_addr);
		
		MongoConn.get_mongo().get_db().getCollection("sensorData").insertOne(unit.get_doc());
		
		info.work();
		//FileUtil.write("CELL => " + farmID + " " + dongID + " " + jeoulID + " " + getTime);
		//FileUtil.write("CELL => " + unit.get_doc().toJson());
	}
	
	//급이, 급수, 외기환경 센서 데이터 적재
	@Override
	protected strictfp void add_ext_data(byte[] packet) {
		
		int idx = 4;
		
		//데이터 초기화 - 외기환경 - 없을 경우 0으로 적재
		double temp = 0.0, temp_1 = 0.0, temp_2 = 0.0, 			//외기온도
				humi = 0.0, humi_1 = 0.0, humi_2 = 0.0, 		//외기습도
				nh3 = 0.0, nh3_1 = 0.0, nh3_2 = 0.0,			//암모니아
				h2s = 0.0, h2s_1 = 0.0, h2s_2 = 0.0,			//황화수소
				pm25 = 0.0, pm25_1 = 0.0, pm25_2 = 0.0,			//초미세먼지
				pm10 = 0.0, pm10_1 = 0.0, pm10_2 = 0.0,			//미세먼지
				w_speed = 0.0, w_speed_1 = 0.0, w_speed_2 = 0.0;	//풍속
		int w_direction = 0;
		
		// -- idx 4부터
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
		
		// -- idx 14부터
		int feed_weight = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		int feed_water = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		
		// -- idx 17부터
		boolean has_ext = false;
		//외기환경 존재할 경우 파싱
		if(packet.length > idx + 2) {
			has_ext = true;
			
			//외기 온도
			byte temp_byte = packet[idx++];
			temp_1 = (temp_byte & 0xFF) > 127 ? (temp_byte & 0x7F) * -1 : (temp_byte & 0xFF);
			temp_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//외기 습도
			humi_1 = packet[idx++] & 0xFF;
			humi_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//풍속
			w_speed_1 = packet[idx++] & 0xFF;
			w_speed_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//풍향
			w_direction = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
			//암모니아
			nh3_1 = packet[idx++] & 0xFF;
			nh3_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//황화수소
			h2s_1 = packet[idx++] & 0xFF;
			h2s_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//초미세먼지
			pm25_1 = packet[idx++] & 0xFF;
			pm25_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			//미세먼지
			pm10_1 = packet[idx++] & 0xFF;
			pm10_2 = FloatCompute.divide((double) (packet[idx++] & 0xFF), 10.0);
			
			temp = temp_1 + temp_2;
			humi = humi_1 + humi_2;
			w_speed = w_speed_1 + w_speed_2;
			nh3 = nh3_1 + nh3_2;
			h2s = h2s_1 + h2s_2;
			pm25 = pm25_1 + pm25_2;
			pm10 = pm10_1 + pm10_2;
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
		
		ext.update(has_ext);
		
		//FileUtil.write("EXT => " + ext.get_doc().toJson());
		//FileUtil.write("EXT => " + farmID + " " + dongID + " " + getTime + " " + feed_weight + " " + feed_water);
	}

	@Override
	//농장에서 전달 받은 원격 명령 응답을 PHP 소켓을 찾아서 전달
	@SuppressWarnings("unchecked")
	protected void pass_to_waiter(byte[] packet) {
		JSONObject jo = new JSONObject();
		
		jo.put("retFarm", farm);
		jo.put("retDong", dong);
		
		int data_len = (int)packet[3] & 0xFF;		//DATA BYTE 길이
		
		String[] data = null;
		
		if(data_len <= 0) {			// 데이터 길이 0일때
			data = new String[1];
			data[0] = "N";
		}
		else {
			data = new String[data_len];
			
			for(int i=0; i<data_len; i++) {
				data[i] = Character.toString((char)packet[4+i]);		//데이터 바이트를 아스키문자열로 치환
			}
		}
		
		int start = 1;			// data byte 중 실제 data 시작 인덱스 - 서브명령 및 저울 번호 제외
		
		switch (data[0]) {
		
		case "F":		//영점 조정
			jo.put("retCell", "0" + data[1]);
			start++;
			break;
		case "M":
		case "V":
		case "S":
		case "R":
			jo.put("retCell", "00");
			break;

		default:
			jo.put("retCell", "00");
			data[0] = "N";
			break;
			
		}
		
		String val = "";
		
		//팬 설정 정보 로직 - 헥사값인 경우 로직
		if(packet[2] == (byte) 0x9C && packet[3] == (byte) 0x04 && packet[4] != (byte) 0x4D) {		// 저울 버전 조회 안겹치게
			val += "WorkTemp = " + String.format("%d", packet[4] & 0xFF);
			val += " / StopTemp = " + String.format("%d", packet[5] & 0xFF);
			val += " / CurrTemp = " + String.format("%d", packet[6] & 0xFF);
			val += " / IsWork = " + String.format("%d", packet[7] & 0xFF);
			
			jo.put("retType", "O");
		}
		if(packet[2] == (byte) 0x90) {
			val += "ACK";
		}
		else {
			for(int j=start; j<data_len; j++) {
				val += data[j];
			}
			jo.put("retType", data[0]);
		}
		
		jo.put("retCode", "S");
		jo.put("retValue", val);
		
		try {
			Socket receiver = P3300Listener.waiter_map.get(farm + dong);
			
			DataOutputStream recv_dos = new DataOutputStream(receiver.getOutputStream());
			recv_dos.writeUTF(jo.toJSONString());
			recv_dos.flush();
			P3300Listener.waiter_map.remove(farm + dong);
			
		}catch(IOException e) {
			FileUtil.write("ERROR => Pass to Client Error in pass_to_waiter()");
		}catch(Exception e) {
			FileUtil.write("ERROR => Pass to Client Error in pass_to_waiter()");
		}
	}
}
