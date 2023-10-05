package server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import util.FileUtil;

public abstract class ServerWorker implements Runnable {
	
	protected String id = "";		// 현재 연결된 id
	
	// 기본 바이트 정보
	protected byte byte_head = (byte) 0xAA;
	protected byte byte_recv_target = (byte) 0x81;
	protected byte byte_send_target = (byte) 0x85;
	protected byte byte_tail = (byte) 0xEE;
	
	// 소켓 및 데이터 송수신 스트림
	protected Socket socket;
	protected DataInputStream dis;
	protected DataOutputStream dos;
	
	//데이터 수신을 위한 변수 선언
	protected byte[] input_bytes = new byte[1024];		//inputstream에서 들어온 바이트를 읽어서 저장함
	protected int input_count = 0;						//inputstream에서 들어온 바이트 배열 길이
	
	protected List<Byte> packet_maker = new ArrayList<Byte>();			//패킷 분할을 위한 리스트
	protected Queue<Byte> receive_buffer = new LinkedList<Byte>();		//패킷 분할을 위해 들어온 바이트를 모두 저장하는 버퍼
	
	protected boolean interrupt = false;
	
	//-----------------------------
	// 생성자
	//-----------------------------
	public ServerWorker(Socket m_socket) {
		
		FileUtil.write("TEST => ServerWorker init");
		this.socket = m_socket;		//소켓 정보를 가져옴

		try {
			// 연결된 소켓으로부터 DataOutputStream 획득
			dos = new DataOutputStream(this.socket.getOutputStream());

			// 연결된 소켓으로부터 DataInputStream 획득
			dis = new DataInputStream(this.socket.getInputStream());

		} catch (IOException e) {
			e.printStackTrace();
			stream_close();
		} catch (Exception e) {
			e.printStackTrace();
			stream_close();
		}
	}
	
	//-----------------------------
	// 쓰레드 run 메소드
	// 데이터 수신, 데이터 파싱, 데이터 저장, 제어기명령 송신, ACK 송신, 명령 전달
	//-----------------------------
	public void run() {
		try {
			// DataInputStream이 존재하면 반복
			while (dis != null && !interrupt) {
				// DataInputStream 값이 있을 때까지 대기
				input_count = dis.read(input_bytes);
				
				//읽어온 값에 오류가 있으면 강제 종료
				if (input_count == -1) {
					//throw new StringIndexOutOfBoundsException();
					break;
				} 
				else {
					receive_work(input_bytes);
				}
			}

		} catch (SocketTimeoutException e) {
			FileUtil.write("ALARM => " + "Socket Timeout : " + socket.getInetAddress().toString() + ":" + socket.getPort());
		} catch (IOException e) {
			FileUtil.write("ALARM => " + "Socket IOException : " + socket.getInetAddress().toString() + ":" + socket.getPort());
		} catch (StringIndexOutOfBoundsException e) {
			FileUtil.write("ALARM => " + "Socket Close : " + socket.getInetAddress().toString() + ":" + socket.getPort());
		} catch (Exception e) {
			e.printStackTrace();
			FileUtil.write("ALARM => " + "Socket Exception : " + socket.getInetAddress().toString() + ":" + socket.getPort());
		}
		finally {
			close_work();
		}
		
	} // run
	
	// 패킷 수신 시 할 동작
	protected abstract void receive_work(byte[] input);
	
	// 쓰레드 종료 시 할 동작
	protected abstract void close_work();
	
	
	// 패킷 만들어서 전송
	protected boolean send(byte[] packet) {
		if(packet != null && packet.length != 0) {	//sparrow 수정사항
			try {
				dos.write(packet);
				dos.flush();
			} catch (IOException e) {
				FileUtil.write("ERROR => IOException in send " + id);
				return false;
			}
			
			return true;
		}
		
		return false;
	}
	
	// 전송 패킷 생성
	protected byte[] make_send_packet(byte comm){
		return make_send_packet(comm, new byte[0]);
	}
	protected byte[] make_send_packet(byte comm, byte data){
		return make_send_packet(comm, new byte[]{data});
	}
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
	
	//crc생성
	protected byte make_crc(byte[] bytes)
    {
        int crcVal = 0;

	    for (int i = 1; i<bytes.length-2; i++) {
		    crcVal ^= bytes[i] & 0xFF;
	    }

	    return (byte) (crcVal & 0xFF);
    }
	
	protected byte make_crc(List<Byte> bytes)
    {
        int crcVal = 0;

	    for (int i = 1; i<bytes.size()-2; i++) {
		    crcVal ^= bytes.get(i) & 0xFF;
	    }

	    return (byte) (crcVal & 0xFF);
    }
	
	// 버퍼에서 바이트를 가져와서 읽고 패킷이 완성될때 까지 작업 반복
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
							&& packet_maker.get(goal-2) == make_crc(packet_maker)	//crc
							&& packet_maker.get(goal-1) == byte_tail) {	//패킷 시작 : AA, 패킷 종료 : EE
						ret = true;
						break;
					}
					else {
						packet_maker.clear();
						FileUtil.write("ERROR => Byte Parsing Error in check_packet() " + id);
					}
				}
			}
			
			len++;
		}
		
		return ret;
	}
	
	//소켓 종료 
	protected void stream_close() {
		try {
			if (dis != null) {
				dis.close();
			}
			if (dos != null) {
				dos.close();
			}

			if (this.socket != null && !this.socket.isClosed()) {
				this.socket.close();
			}

		} catch (IOException e) {
			FileUtil.write("ERROR => stream_close Error");

		} catch (Exception e) {
			FileUtil.write("ERROR => stream_close Error");
		}
	}
}