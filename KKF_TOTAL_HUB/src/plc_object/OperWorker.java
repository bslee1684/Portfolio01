package plc_object;

import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import mini_pc.PlcBridge;
import server.ServerWorker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.MysqlConn;

public class OperWorker extends ServerWorker {
	
	//------------------------------------------------------
	// 기본 프로토콜
	// START  TARGET  COMM  LEN  DATA  CRC  END
	//   AA                                  EE
	//------------------------------------------------------
	
	protected static final byte[] ACK_TO_GW_PACKET = new byte[] {(byte) 0xAA, (byte) 0x85, (byte) 0x90, (byte) 0x00, (byte) 0x15, (byte) 0xEE };
	protected static final int SECURE_LEN = 20;
	
	protected byte byte_head = (byte) 0xAA;
	protected byte byte_recv_target = (byte) 0x81;
	protected byte byte_send_target = (byte) 0x88;
	protected byte byte_tail = (byte) 0xEE;
	
	protected static final byte BYTE_RESP_IS_CONN = (byte) 0x90;		// 30 : 연결 가능 확인 리턴
	protected static final byte BYTE_RESP_COMPLETE = (byte) 0x92;		// 32 : 연결 성공 리턴
	protected static final byte BYTE_RESP_CHECK_ADDR = (byte) 0xA1;		// 41 : 주소 데이터 조회 리턴
	protected static final byte BYTE_RESP_CHECK_BIT = (byte) 0xA3;		// 43 : 비트 데이터 조회 리턴
	protected static final byte BYTE_RESP_WORD_CTR = (byte) 0xA5;		// 45 : 주소 제어 리턴
	protected static final byte BYTE_RESP_BIT_CTR = (byte) 0xA7;		// 47 : 비트 제어 리턴
	
	protected static final byte BYTE_DATA_CONN_COM = (byte) 0x10;		// 연결 성공
	protected static final byte BYTE_DATA_CONN_FAIL = (byte) 0x1A;		// 연결 실패
	protected static final byte BYTE_DATA_WRONG_PSWD = (byte) 0x1B;		// 비밀번호 오류
//	protected static final byte BYTE_CONN_FAIL = (byte) 0x95;	// 농장 연결 불가 오류 바이트
//	protected static final byte BYTE_PASS_ERR = (byte) 0x96;	// 비밀번호 오류 바이트
	
	protected static AtomicInteger instance_count = new AtomicInteger();
	
	protected static final int DAY_TIK = 24 * 60 * 60;	// 하루를 초로 환산
	
	PlcBridge conn_plc = null;
	byte[] secure_byte = null;
	
	public OperWorker(Socket m_socket) {
		super(m_socket);
		FileUtil.write("CONNECT => " + String.valueOf(socket.getInetAddress()) + ":" + socket.getPort() + " connected / Connect Count : " + instance_count.addAndGet(1));
		send(make_send_packet((byte) 0x80)); // ACK 송신
	}
	
	// 패킷 수신시 작업을 구현
	@Override
	protected void receive_work(byte[] input) {
		
		// 수신된 모든 바이트를 버퍼에 담음
		for(int i=0; i<input_count; i++) {
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
				
				FileUtil.write("RECV => " + FileUtil.byte_to_string(packet));
				
				switch (get_byte_idx(packet, 2)) {
				
				case (byte) 0x30: // plc 연결 가능한지 확인
					check_plc(packet);
					break;
					
				case (byte) 0x32: // plc 연결 요청 - 비밀번호 포함
					request_conn(packet);
					break;
					
				case (byte) 0x34: // plc 바로 연결 (테스트)
					test_conn(packet);
					break;
					
				case (byte) 0x41: // 모든 장치 주소 요청
					send_addr_status(packet);
					break;
				
				case (byte) 0x43: // 모든 비트 데이터 요청
//					FileUtil.write("OPER RECV 43 => " + FileUtil.byte_to_string(packet));
					send_bit_status(packet);
					break;
				
				case (byte) 0x45: // 주소 데이터 제어 명령
					deliver_word_ctr(packet);
					break;
				
				case (byte) 0x47: // 비트 데이터 제어 명령
					deliver_bit_ctr(packet);
//					FileUtil.write("OPER RECV 47 => " + FileUtil.byte_to_string(packet));
					break;
					
				case (byte) 0x36: // 출력데이터 전송
					send(make_lcd_data_packet(packet));
//					FileUtil.write("OPER RECV 47 => " + FileUtil.byte_to_string(packet));
					break;
					
				}
				
			}	//check_packet
		} //while(!ByteBuffer.isEmpty())
	}
	
	protected void check_plc(byte[] packet) {
		
		if(conn_plc != null) {
			conn_plc.remove_worker(this);
			conn_plc = null;
		}
		
		String farmID = String.format("KF%04d", ((packet[4] & 0xFF) << 8) | (packet[5] & 0xFF));
		String dongID = String.format("%02d", packet[6] & 0xFF);
		
		if(PlcManager.is_work(farmID, dongID)) { 
			String secure_key = FileUtil.get_random_key(SECURE_LEN);
			secure_byte = new byte[SECURE_LEN];
			for(int i=0; i<SECURE_LEN; i++) {
				secure_byte[i] = (byte) secure_key.charAt(i);
			}
			
			FileUtil.write("TEST => secure_key : " + secure_key);
			send(make_send_packet_with_farm_info(BYTE_RESP_IS_CONN, packet, secure_byte));	
		}
		else {
			send(make_send_packet_with_farm_info(BYTE_RESP_IS_CONN, packet, new byte[]{BYTE_DATA_CONN_FAIL}));	
		}
	}
	
	protected void request_conn(byte[] packet) {
		
		int idx = 4;
		int len = packet[3] & 0xFF;
		
		if(len < SECURE_LEN) {
			send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_WRONG_PSWD}));		
			return;
		}
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		if(PlcManager.is_work(farmID, dongID)) {
			
			StringBuilder sb = new StringBuilder();
			
			for(int i=0; i<SECURE_LEN; i++) {
				byte b = (byte) (secure_byte[i] ^ packet[idx++]);
				
				if(b != (byte) 0xff) {
					sb.append((char)b);
				}
			}
			
			String passwd = sb.toString();
			PlcBridge plc = PlcManager.get_inst().plc_bridge_map.get(farmID + dongID);
			
			if(plc.get_passwd().equals(passwd)) {
				
				FileUtil.write("TEST => passwd : " + passwd);
				
				conn_plc = plc;
				conn_plc.add_worker(this);
				
				send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_CONN_COM}));
			}
			else {	
				send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_WRONG_PSWD}));
			}
			
		}
		else {
			send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		//String passwd = get_ascii_data(Arrays.copyOfRange(packet, idx, len));
	}
	
	protected void test_conn(byte[] packet) {
		
		int idx = 4;
		int len = packet[3] & 0xFF;
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		if(PlcManager.is_work(farmID, dongID)) {
			
			PlcBridge plc = PlcManager.get_inst().plc_bridge_map.get(farmID + dongID);
			
			conn_plc = plc;
			conn_plc.add_worker(this);
			
			send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_CONN_COM}));
			
		}
		else {
			send(make_send_packet_with_farm_info(BYTE_RESP_COMPLETE, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		//String passwd = get_ascii_data(Arrays.copyOfRange(packet, idx, len));
	}
	
	protected void deliver_word_ctr(byte[] packet) {
		
		if(conn_plc == null) {
			send(make_send_packet(BYTE_RESP_WORD_CTR, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		int idx = 4;
		int len = packet[3] & 0xFF;
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		int start = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		List<Integer> int_status = new ArrayList<Integer>();
		List<Byte> byte_status = new ArrayList<Byte>();
		
		for(int i=0; i<len-5; i+=2) {
			
			byte_status.add(packet[idx]);
			byte_status.add(packet[idx+1]);
			
			int val = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
			int_status.add(val);
		}
		
		if(conn_plc != null & conn_plc.get_id().equals(farmID + dongID)) {
			//conn_plc.add_workers(this);
//			System.out.println("start : " + start + " ctr_list : " + ctr_list.toString());
//			conn_plc.comm_buffer.offer(conn_plc.make_write_word_packet(start, int_status));
			conn_plc.send_word(start, int_status, byte_status);
		}
	}
	
	protected void deliver_bit_ctr(byte[] packet) {
		
		if(conn_plc == null) {
			send(make_send_packet(BYTE_RESP_BIT_CTR, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		int idx = 4;
		int len = packet[3] & 0xFF;
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		int start = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		String status = "";
		List<Byte> byte_status = new ArrayList<Byte>();
		
		for(int i=0; i<len-5; i+=2) {
			
			byte_status.add(packet[idx]);
			byte_status.add(packet[idx+1]);
			
			int val = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
			
			if(val == 0) {
				status += "0";
			}
			else {
				status += "1";
			}
		}
		
		if(conn_plc != null & conn_plc.get_id().equals(farmID + dongID)) {
			//conn_plc.add_workers(this);
//			System.out.println("start : " + start + " ctr_list : " + ctr_list.toString());
//			conn_plc.comm_buffer.offer(conn_plc.make_write_bit_packet(start, status));
			conn_plc.send_bit(start, status, byte_status);
		}
		
	}
	
	protected void send_addr_status(byte[] packet) {
		
		String start = DateUtil.get_inst().get_now();
		
		if(conn_plc == null) {
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_ADDR, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
			return;
		}
		
		int idx = 4;
		
		int maxlen = 50;
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		if(conn_plc != null & conn_plc.get_id().equals(farmID + dongID)) {
			HashMap<Integer, Integer> addr_map = conn_plc.get_addr_map();
			
			byte[] data_byte = new byte[maxlen * 4];
			int data_idx = 0;
			int count = 0;
			
			for(Entry<Integer, Integer> entry : addr_map.entrySet()) {
				
				count++;
				
				int addr = entry.getKey();
				int val = entry.getValue();
				
				String hex_addr = String.format("%04x", addr);
				
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
				
				String hex_val = String.format("%04x", val);
				
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
				
				if(count % maxlen == 0) {
					send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_ADDR, packet, data_byte));
					int need = addr_map.size() - count;
					data_byte = new byte[need > maxlen ? maxlen * 4 : need * 4];
					data_idx = 0;
				}
			}
			
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_ADDR, packet, data_byte));
		}
		else {
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_ADDR, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		String end = DateUtil.get_inst().get_now();
		System.out.println("Term : " + DateUtil.get_inst().get_duration(start, end));
		System.out.println("Send Count :" + conn_plc.get_addr_map().size() / maxlen);
		
		
	}
	
	protected void send_bit_status(byte[] packet) {
		
		String start = DateUtil.get_inst().get_now();
		
		if(conn_plc == null) {
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_BIT, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
			return;
		}
		
		int idx = 4;
		
		int maxlen = 50;
		
		String farmID = String.format("KF%04d", ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		String dongID = String.format("%02d", packet[idx++] & 0xFF);
		
		if(conn_plc != null & conn_plc.get_id().equals(farmID + dongID)) {
			HashMap<Integer, Integer> bit_map = conn_plc.get_bit_map();
			
			byte[] data_byte = new byte[maxlen * 4];
			int data_idx = 0;
			int count = 0;
			
			for(Entry<Integer, Integer> entry : bit_map.entrySet()) {
				
				count++;
				
				int addr = entry.getKey();
				int val = entry.getValue();
				
				String hex_addr = String.format("%04x", addr);
				
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
				
				String hex_val = String.format("%04x", val);
				
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
				data_byte[data_idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
				
				if(count % maxlen == 0) {
					send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_BIT, packet, data_byte));
					int need = bit_map.size() - count;
					data_byte = new byte[need > maxlen ? maxlen * 4 : need * 4];
					data_idx = 0;
				}
			}
			
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_BIT, packet, data_byte));
		}
		else {
			send(make_send_packet_with_farm_info(BYTE_RESP_CHECK_BIT, packet, new byte[] {BYTE_DATA_CONN_FAIL}));
		}
		
		String end = DateUtil.get_inst().get_now();
		System.out.println("Term : " + DateUtil.get_inst().get_duration(start, end));
		System.out.println("Send Count :" + conn_plc.get_addr_map().size() / maxlen);
		
		
	}
	
	protected String get_ascii_data(byte[] ascii_packet) {
		StringBuilder builder = new StringBuilder();
		
		for(int i=0; i<ascii_packet.length; i++) {
			builder.append((char)ascii_packet[i]);
		}
		
		return builder.toString();
	}
	
	@Override
	protected void close_work() {
		try {
			conn_plc.remove_worker(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		stream_close();
	}
	
	@Override
	protected boolean send(byte[] packet){
		FileUtil.write("SEND => " + FileUtil.byte_to_string(packet));
		return super.send(packet);
	}
	
	@Override
	protected boolean check_packet() {	//받은 바이트를 분석하여 분기
		boolean ret = false;
		
		int len = packet_maker.size();	//현재 패킷 길이를 가져옴
		int goal = 0;
		
		//바이트 버퍼에 데이터가 남아있으면 진행
		while(!receive_buffer.isEmpty()) {
			
			if(len < 4) {		//data Length(4번째 바이트)까지 무조건 적재
				packet_maker.add(receive_buffer.poll());
				
				//처음 들어온 data가 AA가 아니면 스킵
				if(packet_maker.get(0) != (byte) 0xAA) {
					packet_maker.clear();
					break;
				}
			}
			else {				//data Length 바이트를 분석하여 최종 데이터 길이 까지 적재
				goal = goal == 0 ? (int) (packet_maker.get(3) & 0xFF) + 6 : goal;
				
				packet_maker.add(receive_buffer.poll());
				
				if(goal == len + 1) {		//packetList에 적재된 길이가 올바른 패킷 길이와 같은 경우
					if (packet_maker.get(0) == byte_head		//시작비트
							&& packet_maker.get(1) == byte_recv_target		//타겟 번호
//							&& packet_maker.get(goal-2) == make_crc(packet_maker)	//crc
							&& packet_maker.get(goal-1) == byte_tail) {	//패킷 시작 : AA, 패킷 종료 : EE
						ret = true;
						break;
					}
					else {
						byte[] tt = new byte[packet_maker.size()];
						for(int i=0; i<packet_maker.size(); i++) {
							tt[i] = packet_maker.get(i);
						}
						FileUtil.write("ERROR => Byte Parsing Error in check_packet() " + id + " --> " + FileUtil.byte_to_string(tt));
						packet_maker.clear();
					}
				}
			}
			
			len++;
		}
		
		return ret;
	}
	
	@Override
	protected byte[] make_send_packet(byte comm, byte[] data){
		int len = data.length;
		
		byte[] ret = new byte[6 + len];
		
		ret[0] = byte_head;
		ret[1] = byte_send_target;
		ret[2] = comm;
		ret[3] = (byte) len;
		for(int i=0; i<len; i++){
			ret[4 + i] = data[i];
		}
		ret[ret.length - 2] = make_crc(ret);
		ret[ret.length - 1] = byte_tail;
		
		return ret;
	}
	
	protected byte[] make_send_packet_with_farm_info(byte comm, byte[] recv_packet, byte[] data){
		
		int len = data.length;
		
		byte[] ret = new byte[9 + len];
		
		ret[0] = byte_head;
		ret[1] = byte_send_target;
		ret[2] = comm;
		ret[3] = (byte) (len + 3);
		
		ret[4] = recv_packet[4];
		ret[5] = recv_packet[5];
		ret[6] = recv_packet[6];
		
		for(int i=0; i<len; i++){
			ret[7 + i] = data[i];
		}
		ret[ret.length - 2] = make_crc(ret);
		ret[ret.length - 1] = byte_tail;
		
		return ret;
	}
	
	protected byte get_byte_idx(byte[] bytes, int idx) {
		byte ret = (byte) 0x00;
		
		if(idx > 0 && bytes.length > idx) { 
			ret = bytes[idx];
		}
		
		return ret;
	}
	
	protected byte[] swap_byte_idx(byte[] bytes, int idx, byte swap) {
		if(idx > 0 && bytes.length > idx) {
			bytes[idx] = swap;
		}
		return bytes;
	}
	
	public void send_event(int farm, int dong, int addr, int val) {
		
		byte[] data = new byte[7];
		
		int idx = 0;
		
		String hex_farm = String.format("%04x", farm);
		
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);	
		
		data[idx++] = (byte) dong;
		
		String hex_addr = String.format("%04x", addr);
		
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
		
		String hex_val = String.format("%04x", val);
		
		data[idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
		
		send(make_send_packet(BYTE_RESP_WORD_CTR, data));
	}
	
	public void send_bit_event(int farm, int dong, int addr, int val) {
		
		byte[] data = new byte[7];
		
		int idx = 0;
		
		String hex_farm = String.format("%04x", farm);
		
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);	
		
		data[idx++] = (byte) dong;
		
		String hex_addr = String.format("%04x", addr);
		
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
		
		String hex_val = String.format("%04x", val);
		
		data[idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
		
		send(make_send_packet(BYTE_RESP_BIT_CTR, data));
	}
	
	protected byte[] make_lcd_data_packet(byte[] packet) {
		
		String farm = String.format("KF%04d", ((packet[4] & 0xFF) << 8) | (packet[5] & 0xFF));
		String dong = String.format("%02d", packet[6] & 0xFF);
		
		byte[] ret = new byte[29 + 6];
		
		String now = DateUtil.get_inst().get_now();
		String minus_one = DateUtil.get_inst().get_plus_minus_minute_time(now, -60);
		minus_one = minus_one.substring(0, 13) + ":00:00";
		String minus_day = DateUtil.get_inst().get_plus_minus_minute_time(now, -24 * 60);
		minus_day = minus_day.substring(0, 10) + " 00:00:00";
		
		String sql = "SELECT be.beFarmid, be.beDongid, be.beAvgWeight, be.beDevi, " + 
					"IFNULL(DATEDIFF(IF(cm.cmOutdate is null, current_date(), cm.cmOutdate), cm.cmIndate) + 1, 0) AS inTerm, " + 
					"aw.awWeight, aw.awEstiT2, aw.awEstiT3, sf.sfFeedMax, sf.sfFeed, sf.sfDailyFeed, sf.sfPrevFeed, " + 
					"JSON_EXTRACT(sh.shFeedData, \"$.feed_water\") AS sfHourWater, sf.sfDailyWater, sf.sfPrevWater, si.* " + 
				"FROM buffer_sensor_status AS be " + 
				"LEFT JOIN comein_master AS cm ON cm.cmCode = be.beComeinCode " + 
				"LEFT JOIN set_feeder AS sf ON sf.sfFarmid = be.beFarmid AND sf.sfDongid = be.beDongid " + 
				"LEFT JOIN sensor_history AS sh ON sh.shFarmid = be.beFarmid AND sh.shDongid = be.beDongid AND shDate = \"" + minus_one + "\" " + 
				"LEFT JOIN " +
					 	"(SELECT * FROM avg_weight WHERE awFarmid = \"" + farm + "\" AND awDongid = \"" + dong + "\" AND " +
						"awDate BETWEEN \"" + minus_day + "\" AND \"" + now.substring(0, 10) + " 00:00:00" + "\" ORDER BY awDate DESC LIMIT 1) AS aw " + 
					"ON aw.awFarmid = be.beFarmid " + 
				"LEFT JOIN " + 
				 		"(SELECT siFarmid, MIN(siTempSetDate) , MIN(siHumiSetDate), MIN(siCo2SetDate), MIN(siNh3SetDate) FROM set_iot_cell " +
				 		"WHERE siFarmid = \"" + farm + "\" AND siDongid = \"" + dong + "\" GROUP BY siFarmid, siDongid) AS si " + 
					"ON si.siFarmid = be.beFarmid " + 
				"WHERE be.beFarmid = \"" + farm + "\" AND be.beDongid = \"" + dong + "\";";
		
		Statement state = MysqlConn.get_sql().get_statement();
		ResultSet set = null;
		if(state != null){
			try {
				set = state.executeQuery(sql);
				
				if(set.next()) {
					
					Integer interm = set.getObject("inTerm") != null ? set.getInt("inTerm") : 0;
					Double weight = set.getObject("beAvgWeight") != null ? set.getDouble("beAvgWeight") : 0.0;			// 현재 평균중량
					Double devi = set.getObject("beDevi") != null ? set.getDouble("beDevi") : 0.0;					// 현재 표준편차
					
					Double prevWeight = set.getObject("awWeight") != null ? set.getDouble("awWeight") : 0.0;			// 전일 마지막 평균중량
					Double day1Weight = set.getObject("awEstiT2") != null ? set.getDouble("awEstiT2") : 0.0;		// 내일 예측 평균중량
					Double day2Weight = set.getObject("awEstiT3") != null ? set.getDouble("awEstiT3") : 0.0;		// 모레 예측 평균중량
					
					Integer feedMax = set.getObject("sfFeedMax") != null ? set.getInt("sfFeedMax") : 0;
					Integer feed = set.getObject("sfFeed") != null ? set.getInt("sfFeed") : 0;
					Integer feedDaily = set.getObject("sfDailyFeed") != null ? set.getInt("sfDailyFeed") : 0;
					Integer feedPrev = set.getObject("sfPrevFeed") != null ? set.getInt("sfPrevFeed") : 0;
					Integer waterHour = set.getObject("sfHourWater") != null ? set.getInt("sfHourWater") : 0;
					Integer waterDaily = set.getObject("sfDailyWater") != null ? set.getInt("sfDailyWater") : 0;
					Integer waterPrev = set.getObject("sfPrevWater") != null ? set.getInt("sfPrevWater") : 0;
					
					String siTempSetDate  = set.getObject("MIN(siTempSetDate)") != null ? set.getString("MIN(siTempSetDate)") : "";
					String siHumiSetDate  = set.getObject("MIN(siHumiSetDate)") != null ? set.getString("MIN(siHumiSetDate)") : "";
					String siCo2SetDate  = set.getObject("MIN(siCo2SetDate)") != null ? set.getString("MIN(siCo2SetDate)") : "";
					String siNh3SetDate  = set.getObject("MIN(siNh3SetDate)") != null ? set.getString("MIN(siNh3SetDate)") : "";
					
					int tempTerm = siTempSetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siTempSetDate, now); 
					int humiTerm = siHumiSetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siHumiSetDate, now); 
					int co2Term = siCo2SetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siCo2SetDate, now); 
					int nh3Term = siNh3SetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siNh3SetDate, now); 
					
					// 중량관련 전송
//					String hex_weight = String.format("%04x", Math.round(weight * 10));
//					String hex_devi = String.format("%04x", Math.round(devi * 10));
//					String hex_prev = String.format("%04x", Math.round(prevWeight * 10));
//					String hex_day1 = String.format("%04x", Math.round(day1Weight * 10));
//					String hex_day2 = String.format("%04x", Math.round(day2Weight * 10));
					
					String hex_weight = String.format("%04x", Math.round(weight));
					String hex_devi = String.format("%04x", Math.round(devi));
					String hex_prev = String.format("%04x", Math.round(prevWeight));
					String hex_day1 = String.format("%04x", Math.round(day1Weight));
					String hex_day2 = String.format("%04x", Math.round(day2Weight));
					
					// 급이급수 관련 전송
					String hex_feed_max = String.format("%04x", feedMax);
					String hex_feed = String.format("%04x", feed);
					String hex_feed_daily = String.format("%04x", feedDaily);
					String hex_feed_prev = String.format("%04x", feedPrev);
					String hex_water_hour = String.format("%04x", waterHour);
					String hex_water_daily = String.format("%04x", waterDaily);
					String hex_water_prev = String.format("%04x", waterPrev);
					
					double temp_life = FloatCompute.divide(((DAY_TIK * 365) - tempTerm), (DAY_TIK * 365));
					double humi_life = FloatCompute.divide(((DAY_TIK * 365) - humiTerm), (DAY_TIK * 365));
					double co2_life = FloatCompute.divide(((DAY_TIK * 180) - co2Term), (DAY_TIK * 180));
					double nh3_life = FloatCompute.divide(((DAY_TIK * 180) - nh3Term), (DAY_TIK * 180));
					
					int idx = 0;
					
					ret[idx++] = byte_head;
					ret[idx++] = byte_send_target;
					ret[idx++] = (byte) 0x96;		// LCD 출력 데이터 응답
					ret[idx++] = 29;
					
					ret[idx++] = interm.byteValue();
					ret[idx++] = (byte) Integer.parseInt(hex_weight.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_weight.substring(2, 4), 16);
					ret[idx++] = (byte) Integer.parseInt(hex_devi.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_devi.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_prev.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_prev.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_day1.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_day1.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_day2.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_day2.substring(2, 4), 16);	
					
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_daily.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_daily.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_prev.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_prev.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_hour.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_hour.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_daily.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_daily.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_prev.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_water_prev.substring(2, 4), 16);	
					
					ret[idx++] = temp_life > 0.0 ? (byte) Math.round(temp_life * 100) : 0;
					ret[idx++] = humi_life > 0.0 ? (byte) Math.round(humi_life * 100) : 0;
					ret[idx++] = co2_life > 0.0 ? (byte) Math.round(co2_life * 100) : 0;
					ret[idx++] = nh3_life > 0.0 ? (byte) Math.round(nh3_life * 100) : 0;
					
					ret[idx++] = (byte) 0x00;
					ret[idx++] = byte_tail;
					
					ret[ret.length - 2] = make_crc(ret);
					
//					String msg = "";
//					msg += "일령 => " + interm + "\n";
//					msg += "평균중량 => " + weight + "\n";
//					msg += "표준편차 => " + devi + "\n";
//					msg += "어제 중량 => " + prevWeight + "\n";
//					msg += "내일 예측 => " + day1Weight + "\n";
//					msg += "모레 예측 => " + day2Weight + "\n";
//					msg += "사료빈 용량 => " + feedMax + "\n";
//					msg += "사료빈 잔량 => " + feed + "\n";
//					msg += "오늘 급이량 => " + feedDaily + "\n";
//					msg += "전일 급이량 => " + feedPrev + "\n";
//					msg += "시간당 급수량 => " + waterHour + "\n";
//					msg += "오늘 급수량 => " + waterDaily + "\n";
//					msg += "전일 급수량 => " + waterPrev + "\n";
//					msg += "수명 => " + ret[29] + ", " + ret[30] + ", " + ret[31] + ", " + ret[32] ;
//					
//					System.out.println(msg);
					
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
//				FileUtil.write("ERROR => SQLException Error " + sql + " / Class : sql_conn");
			} catch (Exception e) {
				e.printStackTrace();
//				FileUtil.write("ERROR => Exception Error " + sql + " / Class : sql_conn");
			}
		}
		
		try {
			set.close();
			state.close();
		} catch (SQLException e) {
			FileUtil.write("ERROR => SQLException Error state.close() " + sql + " / Class : sql_conn");
		} catch (Exception e) {
			FileUtil.write("ERROR => Exception Error state.close() " + sql + " / Class : sql_conn");
		}
		
		FileUtil.write("SEND => " + FileUtil.byte_to_string(ret));
		
		return ret;
		
	}
}
