package p3300;

import java.net.Socket;

import compatibility.P3300WorkerVer1;
import server.ServerWorker;
import util.FileUtil;
import ymodem.YModem;

public class P3300Divider extends ServerWorker{

	public P3300Divider(Socket m_socket) {
		super(m_socket);
	}

	@Override
	protected void receive_work(int count, byte[] input) {
		
		// 원격 업데이트 확인
		// ymodem 프로토콜의 시작 명령이 'C'로 되어 있음
		if(input[0] == 'C') {		// 0x43
			FileUtil.write("TEST => Start YModem");
			YModem ymodem = new YModem(dis, dos);
			ymodem.start("./firmware/qtec_gw.bin");
			
			interrupt = true;
		}
		
		// 수신된 모든 바이트를 버퍼에 담음
		for(int i=0; i<count; i++) {
			receive_buffer.offer(input[i]);
		}
		
		//ByteBuffer에서 데이터를 packetList에 하나씩 적재하면서 패킷 끝 부분을 찾음
		if(check_packet()) {
			byte[] packet = new byte[packet_maker.size()];	//패킷이 완성되면 바이트 배열로 옮긴 후 packetList를 클리어
			for(int j=0; j<packet_maker.size(); j++) {
				packet[j] = packet_maker.get(j);
			}
			packet_maker.clear();
			
			//FileUtil.write("RECV => divider : " + FileUtil.byte_to_string(packet));
			
			switch (packet[2]) {
				
			// 원퍼스트 통합 GW
			case (byte) 0x60:
				P3300Listener.get_inst().recreate_worker(new P3300WorkerVer1(socket));
				break;
				
			// 원퍼스트 통합 GW 원격 명령 Client
			case (byte) 0x10:
				P3300Listener.get_inst().recreate_worker(new P3300WorkerVer1(socket, true));
				break;
				
			// 큐텍 통합 GW
			case (byte) 0x61:
				P3300Listener.get_inst().recreate_worker(new P3300Worker(socket, packet));
				break;

			// 큐텍 통합 GW 원격 명령 Client
			case (byte) 0x11:
				P3300Listener.get_inst().recreate_worker(new P3300Worker(socket));
				break;
			}
			
			interrupt = true;		// 데이터 수신 종료 플래그
			
		}	//check_packet
	}

	@Override
	protected void close_work() {
		// TODO Auto-generated method stub
	}
	
	
}
