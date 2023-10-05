package p3500;

import java.net.Socket;

import plc_object.OperWorker;
import server.ServerWorker;
import util.FileUtil;

public class P3500Divider extends ServerWorker{

	public P3500Divider(Socket m_socket) {
		super(m_socket);
	}

	@Override
	protected void receive_work(byte[] input) {
		
		// 수신된 모든 바이트를 버퍼에 담음
		for(int i=0; i<input_count; i++) {
			receive_buffer.offer(input_bytes[i]);
		}
		
		//ByteBuffer에서 데이터를 packetList에 하나씩 적재하면서 패킷 끝 부분을 찾음
		if(check_packet()) {
			byte[] packet = new byte[packet_maker.size()];	//패킷이 완성되면 바이트 배열로 옮긴 후 packetList를 클리어
			for(int j=0; j<packet_maker.size(); j++) {
				packet[j] = packet_maker.get(j);
			}
			packet_maker.clear();
			
			FileUtil.write("RECV => divider : " + FileUtil.byte_to_string(packet));
			
			switch (packet[2]) {
				
			// 원퍼스트 통합 GW
			case (byte) 0x60:
				//P3300Listner.get_inst().recreate_worker(new P3300WorkerVer1(socket));
				break;
				
			// 원퍼스트 통합 GW 원격 명령 Client
			case (byte) 0x10:
				//P3300Listner.get_inst().recreate_worker(new P3300WorkerVer1(socket, true));
				break;
				
			// 큐텍 통합 GW
			case (byte) 0x61:
				P3500Listener.get_inst().recreate_worker(new P3500Worker(socket, packet));
				break;

			// 큐텍 통합 GW 원격 명령 Client
			case (byte) 0x11:
				P3500Listener.get_inst().recreate_worker(new P3500Worker(socket));
				break;
				
			// PLC 명령 클라이언트
			case (byte) 0x20:
				P3500Listener.get_inst().recreate_worker(new OperWorker(socket));
				break;
			}
			
			interrupt = true;
			
		}	//check_packet
	}

	@Override
	protected void close_work() {
		// TODO Auto-generated method stub
	}
	
	
}
