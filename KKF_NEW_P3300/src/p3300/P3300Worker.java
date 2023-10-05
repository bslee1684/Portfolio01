package p3300;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONObject;

import kokofarm.ExternSensor;
import kokofarm.FarmInfo;
import kokofarm.LightSensor;
import kokofarm.MultiFeedSensor;
import kokofarm.SensorUnit;
import server.ServerWorker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.MysqlConn;
import ymodem.YModem;

public class P3300Worker extends ServerWorker {
	
	//------------------------------------------------------
	// 기본 프로토콜
	// START  TARGET  COMM  LEN  DATA  CRC  END
	//   AA                                  EE
	//------------------------------------------------------
	
	// 총 연결된 클라이언트 개수
	protected static AtomicInteger instance_count = new AtomicInteger();
	
	// GW로 응답해주는 ACK 패킷
	protected static final byte[] ACK_TO_GW_PACKET = new byte[] {(byte) 0xAA, (byte) 0x85, (byte) 0x90, (byte) 0x00, (byte) 0x15, (byte) 0xEE };
	
	// 상하층 저울 페어 배열 - 1~12 : 하층부 저울 , 13~24 : 상층부 센서
	protected static final String[] CELL_PAIR_ARR = new String[] {"", "", "", "", "", "", "", "", "", "", "", "", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
	
	protected static final int DAY_TIK = 24 * 60 * 60;	// 하루를 초로 환산
	
	protected FarmInfo info = null;
	protected String farm = "KF0000";
	protected String dong = "00";
	
	// 현재 펌웨어 정보 / 업데이트를 위한 펌웨어 파일 정보
	protected String[] firmware_data;
	
	public P3300Worker(Socket m_socket) {
		super(m_socket);
		FileUtil.write("CONNECT => " + String.valueOf(socket.getInetAddress()) + ":" + socket.getPort() + " connected / Connect Count : " + instance_count.addAndGet(1));
		
		send(ACK_TO_GW_PACKET); // ACK 송신
	}

	public P3300Worker(Socket m_socket, byte[] info_packet) {
		super(m_socket);
		FileUtil.write("CONNECT => " + String.valueOf(socket.getInetAddress()) + ":" + socket.getPort() + " connected / Connect Count : " + instance_count.addAndGet(1));
		
		String farmID = String.format("KF%04d", ((get_byte_idx(info_packet, 4) & 0xFF) << 8) | (get_byte_idx(info_packet, 5) & 0xFF));
		String dongID = String.format("%02d", get_byte_idx(info_packet, 6) & 0xFF);
		FileUtil.write("COMPLETE => Farm Info INIT " + farmID + " " + dongID);
		put_socket_map(farmID, dongID);
		
		info = FarmInfo.get_inst(farmID, dongID);
		
		info_packet = swap_byte_idx(info_packet, 1, (byte) 0x85);
		info_packet = swap_byte_idx(info_packet, 2, (byte) 0x91);
		info_packet = swap_byte_idx(info_packet, info_packet.length - 2, make_crc(info_packet));
		send(info_packet); // ACK 송신
	}
	
	// 패킷 수신시 작업을 구현
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
				
//				FileUtil.write("RECV => " + farm + " " + dong + " : " + FileUtil.byte_to_string(packet));
				
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
						
					case (byte) 0x66: // tft lcd 데이터 요청
						send(make_lcd_data_packet());
						break;
						
					case (byte) 0x67: // 2023-05-15 추가 사료빈 4개인 경우 tft lcd 데이터 요청
						send(make_lcd_multi_feed_data_packet());
						break;
						
					case (byte) 0x4b:		// 2023-05-10 추가 요청사항 - 사료빈 데이터 4개
						send(ACK_TO_GW_PACKET); // ACK 송신
						add_multi_feed_data(packet);
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
						
						// 업데이트 작업
						if(get_byte_idx(packet, 4) == (byte) 0x52) {
							
							FileUtil.write("TEST => Request YModem");
							YModem ymodem = new YModem(dis, dos);
							ymodem.start("./firmware/qtec_gw.bin");
						}
						
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
	protected void close_work() {
		if(info != null) {
			info.delete_inst();
		}
		
		P3300Listener.socket_map.remove(id);
		
		FileUtil.write("DISCONNECT => " + farm + " " + dong + " " + socket.getInetAddress() + ":" + socket.getPort() + " disconnect / connect_count : " + instance_count.addAndGet(-1));
		stream_close();
	}
	
	@Override
	protected boolean send(byte[] packet){
//		FileUtil.write("SEND => " + farm + " " + dong + " : " + FileUtil.byte_to_string(packet));
		
		return super.send(packet);
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
	
	protected void put_socket_map(String farmID, String dongID){
		id = farmID + dongID;
		
		farm = farmID;
		dong = dongID;
		
		P3300Listener.socket_map.put(id, this.socket);
	}
	
	protected byte[] make_time_packet(){
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
	protected byte[] make_firmware_packet(byte[] recv_packet){
		
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
	
	// TFT LCD 데이터 전송
	protected byte[] make_lcd_data_packet() {
		
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
	
	// TFT LCD 데이터 전송 - 2023-05-15 사료빈 4개 인경우 추가
	protected byte[] make_lcd_multi_feed_data_packet() {
		
		// 기존 길이 29에서 12바이트 추가 
		byte[] ret = new byte[41 + 6];
		
		String now = DateUtil.get_inst().get_now();
		String minus_one = DateUtil.get_inst().get_plus_minus_minute_time(now, -60);
		minus_one = minus_one.substring(0, 13) + ":00:00";
		String minus_day = DateUtil.get_inst().get_plus_minus_minute_time(now, -24 * 60);
		minus_day = minus_day.substring(0, 10) + " 00:00:00";
		
		String sql = "SELECT be.beFarmid, be.beDongid, be.beAvgWeight, be.beDevi, " + 
					"IFNULL(DATEDIFF(IF(cm.cmOutdate is null, current_date(), cm.cmOutdate), cm.cmIndate) + 1, 0) AS inTerm, " + 
					"aw.awWeight, aw.awEstiT2, aw.awEstiT3, sf.sfFeedMax, sf.sfFeed, sf.sfDailyFeed, sf.sfPrevFeed, " + 
					"sf.sfFeed01, sf.sfFeed02, sf.sfFeed03, sf.sfFeed04, sf.sfFeedMax01, sf.sfFeedMax02, sf.sfFeedMax03, sf.sfFeedMax04, " +
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
					
					Integer feedMax01 = set.getObject("sfFeedMax01") != null ? set.getInt("sfFeedMax01") : 0;
					Integer feedMax02 = set.getObject("sfFeedMax02") != null ? set.getInt("sfFeedMax02") : 0;
					Integer feedMax03 = set.getObject("sfFeedMax03") != null ? set.getInt("sfFeedMax03") : 0;
					Integer feedMax04 = set.getObject("sfFeedMax04") != null ? set.getInt("sfFeedMax04") : 0;
					
					Integer feed01 = set.getObject("sfFeed01") != null ? set.getInt("sfFeed01") : 0;
					Integer feed02 = set.getObject("sfFeed02") != null ? set.getInt("sfFeed02") : 0;
					Integer feed03 = set.getObject("sfFeed03") != null ? set.getInt("sfFeed03") : 0;
					Integer feed04 = set.getObject("sfFeed04") != null ? set.getInt("sfFeed04") : 0;
					
					String siTempSetDate  = set.getObject("MIN(siTempSetDate)") != null ? set.getString("MIN(siTempSetDate)") : "";
					String siHumiSetDate  = set.getObject("MIN(siHumiSetDate)") != null ? set.getString("MIN(siHumiSetDate)") : "";
					String siCo2SetDate  = set.getObject("MIN(siCo2SetDate)") != null ? set.getString("MIN(siCo2SetDate)") : "";
					String siNh3SetDate  = set.getObject("MIN(siNh3SetDate)") != null ? set.getString("MIN(siNh3SetDate)") : "";
					
					int tempTerm = siTempSetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siTempSetDate, now); 
					int humiTerm = siHumiSetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siHumiSetDate, now); 
					int co2Term = siCo2SetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siCo2SetDate, now); 
					int nh3Term = siNh3SetDate.equals("") ? 0 : DateUtil.get_inst().get_duration(siNh3SetDate, now); 
					
					// 중량관련 전송
//						String hex_weight = String.format("%04x", Math.round(weight * 10));
//						String hex_devi = String.format("%04x", Math.round(devi * 10));
//						String hex_prev = String.format("%04x", Math.round(prevWeight * 10));
//						String hex_day1 = String.format("%04x", Math.round(day1Weight * 10));
//						String hex_day2 = String.format("%04x", Math.round(day2Weight * 10));
					
					String hex_weight = String.format("%04x", Math.round(weight));
					String hex_devi = String.format("%04x", Math.round(devi));
					String hex_prev = String.format("%04x", Math.round(prevWeight));
					String hex_day1 = String.format("%04x", Math.round(day1Weight));
					String hex_day2 = String.format("%04x", Math.round(day2Weight));
					
					// 급이급수 관련 전송
//					String hex_feed_max = String.format("%04x", feedMax);
//					String hex_feed = String.format("%04x", feed);
					String hex_feed_daily = String.format("%04x", feedDaily);
					String hex_feed_prev = String.format("%04x", feedPrev);
					String hex_water_hour = String.format("%04x", waterHour);
					String hex_water_daily = String.format("%04x", waterDaily);
					String hex_water_prev = String.format("%04x", waterPrev);
					
					String hex_feed_max_01 = String.format("%04x", feedMax01);
					String hex_feed_max_02 = String.format("%04x", feedMax02);
					String hex_feed_max_03 = String.format("%04x", feedMax03);
					String hex_feed_max_04 = String.format("%04x", feedMax04);
					String hex_feed_01 = String.format("%04x", feed01);
					String hex_feed_02 = String.format("%04x", feed02);
					String hex_feed_03 = String.format("%04x", feed03);
					String hex_feed_04 = String.format("%04x", feed04);
					
					double temp_life = FloatCompute.divide(((DAY_TIK * 365) - tempTerm), (DAY_TIK * 365));
					double humi_life = FloatCompute.divide(((DAY_TIK * 365) - humiTerm), (DAY_TIK * 365));
					double co2_life = FloatCompute.divide(((DAY_TIK * 180) - co2Term), (DAY_TIK * 180));
					double nh3_life = FloatCompute.divide(((DAY_TIK * 180) - nh3Term), (DAY_TIK * 180));
					
					int idx = 0;
					
					ret[idx++] = byte_head;
					ret[idx++] = byte_send_target;
					ret[idx++] = (byte) 0x97;		// LCD 출력 데이터 응답
					ret[idx++] = 41;
					
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
					
					// 사료빈 하나에서 총 4개로 수정
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_01.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_01.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_01.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_01.substring(2, 4), 16);	
					
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_02.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_02.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_02.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_02.substring(2, 4), 16);	
					
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_03.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_03.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_03.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_03.substring(2, 4), 16);	
					
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_04.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_max_04.substring(2, 4), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_04.substring(0, 2), 16);	
					ret[idx++] = (byte) Integer.parseInt(hex_feed_04.substring(2, 4), 16);	
					
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
					
//						String msg = "";
//						msg += "일령 => " + interm + "\n";
//						msg += "평균중량 => " + weight + "\n";
//						msg += "표준편차 => " + devi + "\n";
//						msg += "어제 중량 => " + prevWeight + "\n";
//						msg += "내일 예측 => " + day1Weight + "\n";
//						msg += "모레 예측 => " + day2Weight + "\n";
//						msg += "사료빈 용량 01 => " + feedMax01 + "\n";
//						msg += "사료빈 잔량 01 => " + feed01 + "\n";
//						msg += "사료빈 용량 02 => " + feedMax02 + "\n";
//						msg += "사료빈 잔량 02 => " + feed02 + "\n";
//						msg += "사료빈 용량 03 => " + feedMax03 + "\n";
//						msg += "사료빈 잔량 03 => " + feed03 + "\n";
//						msg += "사료빈 용량 04 => " + feedMax04 + "\n";
//						msg += "사료빈 잔량 04 => " + feed04 + "\n";
//						msg += "오늘 급이량 => " + feedDaily + "\n";
//						msg += "전일 급이량 => " + feedPrev + "\n";
//						msg += "시간당 급수량 => " + waterHour + "\n";
//						msg += "오늘 급수량 => " + waterDaily + "\n";
//						msg += "전일 급수량 => " + waterPrev + "\n";
//						msg += "수명 => " + ret[29] + ", " + ret[30] + ", " + ret[31] + ", " + ret[32] ;
						
//						System.out.println(msg);
					
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
//					FileUtil.write("ERROR => SQLException Error " + sql + " / Class : sql_conn");
			} catch (Exception e) {
				e.printStackTrace();
//					FileUtil.write("ERROR => Exception Error " + sql + " / Class : sql_conn");
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
		
//		FileUtil.write("SEND => " + FileUtil.byte_to_string(ret));
		
		return ret;
		
	}
	
	//펌웨어 데이터를 읽어오는 메소드
	protected void read_firmware() {
		String firmware_path = "./firmware/gw.hex";
		firmware_data = FileUtil.file_read(firmware_path).split("\\n");
//		firmware_data = Arrays.asList(data);
	}
	
	// PHP에서 원격명령 전송 시 연결된 농장의 소켓을 찾아 전달함
	@SuppressWarnings("unchecked")
	protected void send_order(byte[] packet){
		String target_farm = String.format("KF%04d", ((packet[4] & 0xFF) << 8) | (packet[5] & 0xFF));
		String target_dong = String.format("%02d", packet[6] & 0xFF);
		
		String key = target_farm + target_dong;
		
		try {
			if(P3300Listener.socket_map.containsKey(key)) {		//현재 접속된 통합G/W 리스트에 명령 대상이 존재하면 실행
				Socket target_socket = P3300Listener.socket_map.get(key);
				
				if(target_socket.isConnected()) {
					DataOutputStream target_dos = new DataOutputStream(target_socket.getOutputStream());
					
					P3300Listener.waiter_map.put(key, this.socket);
					
					target_dos.write(make_order_packet(packet));
					target_dos.flush();
					
					FileUtil.write("COMPLETE => Send to " + key + " COMM : 0x" + String.format("%02X", packet[2] & 0xff));
					
					return;
				}
				else{
					FileUtil.write("ERROR => Exception " + key + " is not connect");
				}
			}
			else{
				FileUtil.write("ERROR => Exception " + key + " is not connect");
			}
			
		}catch(IOException e) {
			FileUtil.write("ERROR => IOException " + key + " send_order Error");
		}catch(Exception e) {
			FileUtil.write("ERROR => Exception " + key + " send_order Error");
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
	protected byte[] make_order_packet(byte[] packet) throws Exception {
		
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
	protected void pass_to_waiter(byte[] packet) {
		JSONObject jo = new JSONObject();
		
		jo.put("retFarm", farm);
		jo.put("retDong", dong);
		
		int data_len = (int)packet[3] & 0xFF;		//DATA BYTE 길이
		
		StringBuilder data_builder = new StringBuilder();
		
		for(int i=0; i<data_len; i++){
			data_builder.append((char)packet[4+i]);
		}
		
		String data = data_builder.toString();
		
		//FileUtil.write("TEST => " + data);
		
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
			Socket waiter = P3300Listener.waiter_map.get(id);
			
			FileUtil.write("TEST => " + jo.toJSONString());
			
			DataOutputStream waiter_dos = new DataOutputStream(waiter.getOutputStream());
			waiter_dos.writeUTF(jo.toJSONString());
			waiter_dos.flush();
			P3300Listener.waiter_map.remove(id);
			
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
		
		//FileUtil.write("TEST => " + FileUtil.byte_to_string(packet));
		
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
		
		info = FarmInfo.get_inst(farmID, dongID);
		
		temp = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		humi = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		co = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		nh = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		dust_high = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		dust_low = ((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF);
		
		// 20220211 습도 보정 요청 사항 적용 +17%
		// 20220314 습도 보정 수정 +17% +6%
		if(humi < 60000) {
			humi = humi + 230.0;
			humi = humi > 990.0 ? 990.0 : humi; 
		}
		
		temp = check_err_data((int) temp, 10.0);
		humi = check_err_data((int) humi, 10.0);
		nh = check_err_data((int) nh, 10.0);
		
		co = check_err_data((int) co, 1.0);
		dust_high = check_err_data((int) dust_high, 1.0);
		dust_low = check_err_data((int) dust_low, 1.0);
		
		//FileUtil.write("TEST => high : " + dust_high + " low : " + dust_low + " / " + farmID + dongID + jeoulID );

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
		
		if(pair_num.equals("")) {		// 하층부 저울인경우
			info.set_updatable(true);
		}
		else {		// 상층부인 경우
			if(info.get_cell_map().containsKey(pair_num)) {	// 211028 이병선 수정 - 하층부 센서가 없는 경우 에러남 (하층부가 있는지 확인)
				info.get_cell(pair_num).set_val("dust", dust_low);
			}
		}

		for (int z = 0; z < 60; z++) {
			unit.set_val("w" + String.format("%02d", z + 1), w[z]);
		}
		
		unit.set_max_weight(max_weight);
		
		info.add_sensor_data_list(unit.get_doc());		// 몽고db 업데이트 대기열에 적재
		info.set_last_sensor_date(getTime); 			// 최종 수집시간
		
//		FileUtil.write("CELL => " + farmID + " " + dongID + " " + jeoulID + " high : " + dust_high + " low : " + dust_low);
		//FileUtil.write("CELL => " + unit.get_doc().toJson());
	}
	
	//급이, 급수, 외기환경 센서 데이터 적재
	protected strictfp void add_ext_data(byte[] packet) {
		
		int idx = 4;
		
		//데이터 초기화 - 외기환경 - 없을 경우 0으로 적재
		double temp = 0.0, humi = 0.0, nh3 = 0.0, h2s = 0.0, w_speed = 0.0, solar = 0.0, pm10 = 0.0, pm25 = 0.0;
		int w_direction = 0;
		
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
		
		//FileUtil.write("TEST => feed_weight : " + feed_weight + " / feed_water : " + feed_water);
		
		feed_weight = check_err_data(feed_weight);
		feed_water = check_err_data(feed_water);
		
//		if(farmID.equals("KF0008")) {
//			FileUtil.write("PACKET => " + FileUtil.byte_to_string(packet));
//			FileUtil.write("TEST => " + farmID + dongID + " " + getTime + " feed_weight : " + feed_weight +  " feed_water : " + feed_water);
//		}
		
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
			
			pm10 = check_err_data((int) pm10, 1.0);
			pm25 = check_err_data((int) pm25, 1.0);
			w_direction = check_err_data(w_direction);
			
			// lux를 W/m2로 변환 :: 100lux = 1W/m2
			solar = check_err_data((int) solar, 100.0);
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
		//FileUtil.write("EXT => " + farmID + " " + dongID + " " + getTime + " " + feed_weight + " " + feed_water);
	}
	
	// 2023-05-10 추가 요청사항 - 사료빈 데이터 4개 데이터 수신
	protected strictfp void add_multi_feed_data(byte[] packet) {
		
		int idx = 4;
		
		//데이터 초기화 - 외기환경 - 없을 경우 0으로 적재
		double temp = 0.0, humi = 0.0, nh3 = 0.0, h2s = 0.0, w_speed = 0.0, solar = 0.0, pm10 = 0.0, pm25 = 0.0;
		int w_direction = 0;
		
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
		
		// -- idx 13부터 사료빈 4개 20까지
		int[] feed_weight = new int[4];
		for(int n=0; n<4; n++) {
			feed_weight[n] = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
			feed_weight[n] = check_err_data(feed_weight[n]);
		}
		
		// -- idx 21부터 유량센서 
		int feed_water = (((packet[idx++] & 0xFF) << 8) | (packet[idx++] & 0xFF));
		feed_water = check_err_data(feed_water);
		
		// -- idx 23부터 외기환경
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
			
			pm10 = check_err_data((int) pm10, 1.0);
			pm25 = check_err_data((int) pm25, 1.0);
			w_direction = check_err_data(w_direction);
			
			// lux를 W/m2로 변환 :: 100lux = 1W/m2
			solar = check_err_data((int) solar, 100.0);
		}
		
		// 2023-05-15 테스트 로그
//		String te = "";
//		for(int n=0; n<4; n++) {
//			te += (n+1) + " => " + feed_weight[n] + ", ";
//		}
//		FileUtil.write("FEED => "+ getTime + "  /  " + te);
		
		MultiFeedSensor ext = info.get_multi_feed_sensor();
		ext.set_val("_id", farmID + dongID + "_" + DateUtil.get_inst().get_timestamp(year, month, date, hour, min, sec));
		ext.set_val("farmID", farmID);
		ext.set_val("dongID", dongID);
		ext.set_val("getTime", getTime);
		
		ext.set_val("feedWeight", feed_weight[0]);
		ext.set_val("feedWeightval", 0);
		ext.set_val("feedWeight_02", feed_weight[1]);
		ext.set_val("feedWeightval_02", 0);
		ext.set_val("feedWeight_03", feed_weight[2]);
		ext.set_val("feedWeightval_03", 0);
		ext.set_val("feedWeight_04", feed_weight[3]);
		ext.set_val("feedWeightval_04", 0);
		
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
		//FileUtil.write("EXT => " + farmID + " " + dongID + " " + getTime + " " + feed_weight + " " + feed_water);
	}
	
	// 조도 센서 데이터 적재
	protected strictfp void add_light_data(byte[] packet) {
		
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
		//FileUtil.write("LIGHT => " + farmID + " " + dongID + " " + getTime);
	}
	
	protected int get_code_data(byte byte1, byte byte2) {
//		int a = (byte1 & 0x7F);
//		int b = (byte2 & 0xFF);
		
		int ret = ((byte1 & 0x7F) << 8) | (byte2 & 0xFF);
		ret = (byte1 & 0xFF) > 127 ? ret * -1 : ret;
		
		return ret;
	}
	
	protected int check_err_data(int val) {
		
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
	
	protected double check_err_data(int val, double div) {
		
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
