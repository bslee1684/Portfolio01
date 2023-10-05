package mini_pc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;

import plc_object.BitInfo;
import plc_object.DataUpdator;
import plc_object.OperWorker;
import plc_object.PlcManager;
import plc_object.UnitInfo;
import util.DateUtil;
import util.FileUtil;
import util.SimpleTimer;

public class MiniClient implements PlcBridge{
	
	protected static ExecutorService executor = Executors.newCachedThreadPool();
	
	private static final byte BYTE_HEAD = (byte) 0xAA;
	private static final byte BYTE_RECV_TARGET = (byte) 0x88;
	private static final byte BYTE_SEND_TARGET = (byte) 0x81;
	private static final byte BYTE_TAIL = (byte) 0xEE;
	
	protected String farmID;
	protected String dongID;
	protected String host;
	protected int port;
	
	protected String passwd;
	
	protected Socket socket = null;
	protected DataInputStream dis = null;
	protected DataOutputStream dos = null;
	
	protected SimpleTimer send_timer = null;
	
	protected byte[] input_bytes = null;
	
	protected Vector<OperWorker> worker_list = new Vector<OperWorker>();							// 연결된 클라이언트 리스트
	
	protected List<Byte> packet_maker = new ArrayList<Byte>();			//패킷 분할을 위한 리스트
	protected Queue<Byte> receive_buffer = new LinkedList<Byte>();		//패킷 분할을 위해 들어온 바이트를 모두 저장하는 버퍼
	
	protected String comein_code = "";
	
	private LinkedHashMap<Integer, Integer> addr_map = new LinkedHashMap<Integer, Integer>();
	private LinkedHashMap<Integer, Integer> bit_map = new LinkedHashMap<Integer, Integer>();
	protected LinkedHashMap<Integer, Integer> breed_map = new LinkedHashMap<Integer, Integer>();
	
	public MiniClient(String host, int port, String farmID, String dongID){
		
		this.host = host;
		this.port = port;
		this.farmID = farmID;
		this.dongID = dongID;
		
		input_bytes = new byte[1024];
		send_timer = SimpleTimer.create_inst();
		
		FileUtil.write("INIT => miniClient Init : " + farmID + dongID);
	}

	@Override
	public void start() {
		
		if(is_connect()) {
			return;
		}
		
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				
				if(!connect()) {
					return;
				}
				
				work();
			}
		});
	}
	
	protected void work() {
		
		try {
			// DataInputStream이 존재하면 반복
			while (dis != null) {
				// DataInputStream 값이 있을 때까지 대기
				int input_count = dis.read(input_bytes);
				
				//읽어온 값에 오류가 있으면 강제 종료
				if (input_count == -1) {
					//throw new StringIndexOutOfBoundsException();
					break;
				} 
				else {
					// 수신된 모든 바이트를 버퍼에 담음
					for(int i=0; i<input_count; i++) {
						receive_buffer.offer(input_bytes[i]);
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
							
//							FileUtil.write("RECV => " + FileUtil.byte_to_string(packet));
							
							switch (packet[2]) {
							
							case (byte) 0x80: // 연결 완료
								send(make_get_addr_data());
								send(make_get_bit_data());
							break;
							
							case (byte) 0xA1: // 전체 주소 받기
								parse_addr_data(packet);
//								send(make_get_bit_data());
//								FileUtil.write("RECV => 0xA1");
								break;
								
							case (byte) 0xA3: // 전체 비트 받기
								parse_bit_data(packet);
//								FileUtil.write("RECV => 0xA3");
								break;
								
							case (byte) 0xA5: // 이벤트 데이터 수신
								parse_event_data(packet);
//								FileUtil.write("RECV => 0xA5");
								break;
								
							case (byte) 0xA7: // 비트 이벤트 데이터 수신
								parse_bit_event_data(packet);
//								FileUtil.write("RECV => 0xA7");
								break;
							}
							
						}	//check_packet
					} //while(!ByteBuffer.isEmpty())
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
			delete_inst();
		}
	}
	
	protected boolean connect() {
		boolean ret = true;
		
		try {
			socket = new Socket(host, port);
			dis = new DataInputStream(socket.getInputStream());
			dos = new DataOutputStream(socket.getOutputStream());
			
			send(make_init_data());
			
			FileUtil.write("CONNECT => miniClient Connect : " + farmID + dongID);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			ret = false;
		} catch (IOException e) {
			e.printStackTrace();
			ret = false;
		} catch (Exception e) {
			ret = false;
			FileUtil.write("FAIL => Socket Connect Fail : " + farmID + dongID);
		}
		
		return ret;
	} 

	@Override
	public boolean is_connect() {
		
		if(socket == null) {
			return false;
		}
		
		return socket.isConnected();
	}

	@Override
	public void add_worker(OperWorker worker) {
		worker_list.add(worker);
		FileUtil.write("ADD => worker_list size : " + worker_list.size());
	}

	@Override
	public void remove_worker(OperWorker worker) {
		worker_list.remove(worker);
		FileUtil.write("REMOVE => worker_list size : " + worker_list.size());
	}

	@Override
	public HashMap<Integer, Integer> get_addr_map() {
		
		return addr_map;
	}

	@Override
	public HashMap<Integer, Integer> get_bit_map() {
		return bit_map;
	}

	@Override
	public void set_passwd(String passwd) {
		this.passwd = passwd;
	}

	@Override
	public String get_passwd() {
		return passwd;
	}

	@Override
	public String get_id() {
		return farmID + dongID;
	}

	@Override
	public void delete_inst() {
		
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
		
		FileUtil.write("DELETE => " + farmID + dongID + " delete_inst()");
		PlcManager.get_inst().plc_bridge_map.remove(farmID + dongID);
		
	}
	
	private void send(byte[] packet) {
		
		if(socket != null && socket.isConnected()) {
			try {
//				FileUtil.write("SEND => " + FileUtil.byte_to_string(packet));
				dos.write(packet);
				dos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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
					if (packet_maker.get(0) == BYTE_HEAD		//시작비트
							&& packet_maker.get(1) == BYTE_RECV_TARGET		//타겟 번호
							&& packet_maker.get(goal-2) == make_crc(packet_maker)	//crc
							&& packet_maker.get(goal-1) == BYTE_TAIL) {	//패킷 시작 : AA, 패킷 종료 : EE
						ret = true;
						break;
					}
					else {
						packet_maker.clear();
						FileUtil.write("ERROR => Byte Parsing Error in check_packet() " + farmID + " " + dongID);
					}
				}
			}
			
			len++;
		}
		
		return ret;
	}
	
	private byte[] make_send_packet(byte comm, byte[] data){
		int len = data.length;
		
		byte[] ret = new byte[6 + len];
		
		ret[0] = BYTE_HEAD;
		ret[1] = BYTE_SEND_TARGET;
		ret[2] = comm;
		ret[3] = (byte) len;
		for(int i=0; i<len; i++){
			ret[4 + i] = data[i];
		}
		ret[ret.length - 2] = make_crc(ret);
		ret[ret.length - 1] = BYTE_TAIL;
		
		return ret;
	}
	
	private byte[] make_init_data() {
		return make_send_packet((byte) 0x20, new byte[0]);
	}
	
	private byte[] make_get_addr_data() {
		byte[] data = new byte[3];
		
		String hex_farm = String.format("%04x", Integer.parseInt(farmID.substring(2)));
		
		data[0] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[1] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);			
		data[2] = (byte) Integer.parseInt(dongID);		
		
		return make_send_packet((byte) 0x41, data);
	}
	
	private byte[] make_get_bit_data() {
		byte[] data = new byte[3];
		
		String hex_farm = String.format("%04x", Integer.parseInt(farmID.substring(2)));
		
		data[0] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[1] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);			
		data[2] = (byte) Integer.parseInt(dongID);		
		
		return make_send_packet((byte) 0x43, data);
	}
	
	private void parse_event_data(byte[] packet) {
		
		int addr = ((packet[7] & 0xFF) << 8) | (packet[8] & 0xFF);
		int val = ((packet[9]) << 8) | (packet[10]  & 0xFF);
		
		UnitInfo info = PlcManager.get_unit_info(addr);
		
		if(info != null) {
			
			String now = DateUtil.get_inst().get_now();
			long timestamp = DateUtil.get_inst().get_timestamp(now);
			
			if(addr_map.containsKey(addr)) {		// 최초가 아닌경우
				int prev = addr_map.get(addr);
				if(prev != val) {
					Document doc = info.get_event_doc(farmID, dongID, now, timestamp, val);
					DataUpdator.get_inst().event_doc_queue.offer(doc);
					event_send_all(addr, val);
					FileUtil.write("EVENT => " + farmID + " " + dongID + " -- " + addr + " : " + prev + " to " + val);
				}
			}
			else {
				if(val != -1) {
					if(info.get_property().equals("D")) {
						Document doc = info.get_event_doc(farmID, dongID, now, timestamp, val);
						DataUpdator.get_inst().event_doc_queue.offer(doc);
					}
				}
			}
			
			addr_map.put(addr, val);
		}
		
//		int prev = addr_map.getOrDefault(addr, -1);
//		addr_map.put(addr, val);
//		FileUtil.write("EVENT => " + addr + " : " + prev + " to " + val);
	}
	
	private void parse_bit_event_data(byte[] packet) {
		
		int addr = ((packet[7] & 0xFF) << 8) | (packet[8] & 0xFF);
		int val = ((packet[9]) << 8) | (packet[10]  & 0xFF);
		
		BitInfo bit_info = PlcManager.get_bit_info(addr);
		
		if(bit_info != null) {
			
			String now = DateUtil.get_inst().get_now();
			long timestamp = DateUtil.get_inst().get_timestamp(now);
			
			if(bit_map.containsKey(addr)) {		// 최초가 아닌경우
				int prev = bit_map.get(addr);
				if(prev != val) {
					Document doc = bit_info.get_event_doc(farmID, dongID, now, timestamp, val);
					DataUpdator.get_inst().event_doc_queue.offer(doc);
					bit_event_send_all(addr, val);
					FileUtil.write("BIT EVENT => " + farmID + " " + dongID + " -- " + addr + " : " + prev + " to " + val);
				}
			}
			
			bit_map.put(addr, val);
		}
		
//		int prev = bit_map.getOrDefault(addr, -1);
//		bit_map.put(addr, val);
//		FileUtil.write("EVENT => " + addr + " : " + prev + " to " + val);
	}
	
	private void parse_addr_data(byte[] packet) {
		
		int start = 7;
		
		if(packet.length > 7) {
			
			int len = (packet[3] & 0xff) - 3;
			
			for(int i=0; i<len; i+=4) {
				int addr = ((packet[start + i] & 0xFF) << 8) | (packet[start + 1 + i] & 0xFF);
				int val = ((packet[start + 2 + i]) << 8) | (packet[start + 3 + i] & 0xFF);
				
//				FileUtil.write("addr => " + addr + " : " + val);
				
				addr_map.put(addr, val);
			}
			
		}
		
	}
	
	private void parse_bit_data(byte[] packet) {
		
		int start = 7;
		
		if(packet.length > 7) {
			
			int len = (packet[3] & 0xff) - 3;
			
			for(int i=0; i<len; i+=4) {
				int addr = ((packet[start + i] & 0xFF) << 8) | (packet[start + 1 + i] & 0xFF);
				int val = ((packet[start + 2 + i]) << 8) | (packet[start + 3 + i] & 0xFF);
				
//				FileUtil.write("bit => " + addr + " : " + val);
				
				bit_map.put(addr, val);
			}
			
		}
		
	}
	
	private byte[] make_event_packet(int addr, List<Byte> byte_status) {
		byte[] data = new byte[5 + byte_status.size()];
		
		int idx = 0;
		
		String hex_farm = String.format("%04x", Integer.parseInt(farmID.substring(2)));
		
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);			
		data[idx++] = (byte) Integer.parseInt(dongID);
		
		String hex_addr = String.format("%04x", addr);
		
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
		
		for(byte b : byte_status) {
			data[idx++] = b;	
		}
		
//		data[idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
//		data[idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
		
		return make_send_packet((byte) 0x45, data);
	}
	
	private byte[] make_bit_event_packet(int addr, List<Byte> byte_status) {
		
		byte[] data = new byte[5 + byte_status.size()];
		
		int idx = 0;
		
		String hex_farm = String.format("%04x", Integer.parseInt(farmID.substring(2)));
		
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_farm.substring(2, 4), 16);			
		data[idx++] = (byte) Integer.parseInt(dongID);
		
		String hex_addr = String.format("%04x", addr);
		
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hex_addr.substring(2, 4), 16);	
		
		for(byte b : byte_status) {
			data[idx++] = b;	
		}
		
//		String hex_val = String.format("%04x", val);
//		
//		data[idx++] = (byte) Integer.parseInt(hex_val.substring(0, 2), 16);			
//		data[idx++] = (byte) Integer.parseInt(hex_val.substring(2, 4), 16);	
		
		return make_send_packet((byte) 0x47, data);
	}
	
	public void send_word(int start, int[] data) {
		
		List<Byte> byte_status = new ArrayList<Byte>();
		
		for(int out : data) {
			String hexout = String.format("%04x", out);
			Byte high = (byte) Integer.parseInt(hexout.substring(0, 2), 16);			
			Byte low = (byte) Integer.parseInt(hexout.substring(2, 4), 16);	
			
			byte_status.add(high);
			byte_status.add(low);
		}
		
		send(make_event_packet(start, byte_status));
	}
	
	public void send_word(int start, List<Integer> int_status, List<Byte> byte_status) {
//		FileUtil.write(byte_to_string(make_event_packet(addr, val)));
		send(make_event_packet(start, byte_status));
	}
	
	public void send_bit(int start, String status, List<Byte> byte_status) {
//		FileUtil.write(byte_to_string(make_bit_event_packet(addr, val)));
		send(make_bit_event_packet(start, byte_status));
	}
	
	protected void event_send_all(int addr, int val) {
		
		int f = Integer.parseInt(farmID.substring(2));
		int d = Integer.parseInt(dongID);
		
		for(OperWorker worker : worker_list) {
			worker.send_event(f, d, addr, val);
		}
		
		FileUtil.write("SEND ALL => worker_list size : " + worker_list.size());
	}
	
	protected void bit_event_send_all(int addr, int val) {
		
		int f = Integer.parseInt(farmID.substring(2));
		int d = Integer.parseInt(dongID);
		
		for(OperWorker worker : worker_list) {
			worker.send_bit_event(f, d, addr, val);
		}
	}

	@Override
	public void set_comein_code(String code) {
		this.comein_code = code;
	}

	@Override
	public void set_breed_map(int addr, int val) {
		breed_map.put(addr, val);
	}


}
