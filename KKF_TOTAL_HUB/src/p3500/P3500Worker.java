package p3500;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.simple.JSONObject;

import server.ServerWorker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;

public class P3500Worker extends ServerWorker {
	
	//------------------------------------------------------
	// 기본 프로토콜
	// START  TARGET  COMM  LEN  DATA  CRC  END
	//   AA                                  EE
	//------------------------------------------------------
	
	protected static AtomicInteger instance_count = new AtomicInteger();
	
	protected static final byte[] ACK_TO_GW_PACKET = new byte[] {(byte) 0xAA, (byte) 0x85, (byte) 0x90, (byte) 0x00, (byte) 0x15, (byte) 0xEE };
	
	protected String farm = "KF0000";
	protected String dong = "00";
	
	// 현재 펌웨어 정보 / 업데이트를 위한 펌웨어 파일 정보
	protected String[] firmware_data;
	
	public P3500Worker(Socket m_socket) {
		super(m_socket);
		FileUtil.write("CONNECT => " + String.valueOf(socket.getInetAddress()) + ":" + socket.getPort() + " connected / Connect Count : " + instance_count.addAndGet(1));
		
		send(ACK_TO_GW_PACKET); // ACK 송신
	}

	public P3500Worker(Socket m_socket, byte[] info_packet) {
		super(m_socket);
		FileUtil.write("CONNECT => " + String.valueOf(socket.getInetAddress()) + ":" + socket.getPort() + " connected / Connect Count : " + instance_count.addAndGet(1));
		
		String farmID = String.format("KF%04d", ((get_byte_idx(info_packet, 4) & 0xFF) << 8) | (get_byte_idx(info_packet, 5) & 0xFF));
		String dongID = String.format("%02d", get_byte_idx(info_packet, 6) & 0xFF);
		FileUtil.write("COMPLETE => Farm Info Receive " + farmID + " " + dongID);
		
		info_packet = swap_byte_idx(info_packet, 1, (byte) 0x85);
		info_packet = swap_byte_idx(info_packet, 2, (byte) 0x91);
		info_packet = swap_byte_idx(info_packet, info_packet.length - 2, make_crc(info_packet));
		send(info_packet); // ACK 송신
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
				
				FileUtil.write("RECV => " + farm + " " + dong + " : " + FileUtil.byte_to_string(packet));
				
			}	//check_packet
		} //while(!ByteBuffer.isEmpty())
	}
	
	@Override
	protected void close_work() {
		FileUtil.write("DISCONNECT => " + socket.getInetAddress() + ":" + socket.getPort() + " disconnect / connect_count : " + instance_count.addAndGet(-1));
		stream_close();
	}
	
	@Override
	protected boolean send(byte[] packet){
		//FileUtil.write("SEND => " + farm + " " + dong + " : " + FileUtil.byte_to_string(packet));
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
	
	
}
