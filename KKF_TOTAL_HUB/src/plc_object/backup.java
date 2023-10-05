package plc_object;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;

import util.DateUtil;
import util.FileUtil;
import util.ModbusUtil;
import util.MongoConn;
import util.MysqlConn;
import util.SimpleTimer;

public class backup extends ModbusUtil{
	
	protected static final int DEFAULT_IDX = 9001;
	protected static final int PERIOD = 50;
	protected static ExecutorService excutor = Executors.newCachedThreadPool();
	
	protected String farmID;
	protected String dongID;
	protected String host;
	protected int port;
	
	protected String passwd;
	
	protected Socket socket = null;
	protected DataInputStream dis = null;
	protected DataOutputStream dos = null;
	
	protected ConcurrentLinkedQueue<byte[]> comm_buffer = new ConcurrentLinkedQueue<byte[]>();		// 명령 및 조회 대기 풀
	protected Vector<OperWorker> worker_list = new Vector<OperWorker>();							// 연결된 클라이언트 리스트
	
	protected boolean comein_status = false;
		
	protected List<byte[]> read_word_packet_list = null;
	protected int event_read_idx = 3;
	
	protected SimpleTimer send_timer = null;
	protected int term = 0;
	
	protected String comein_code = "";
	protected HashMap<Integer, Integer> addr_map = new HashMap<Integer, Integer>();
	protected HashMap<Integer, Integer> breed_map = new HashMap<Integer, Integer>();
	
	protected byte[] input_bytes = null;
	
	public backup(String host, int port, String farmID, String dongID){
		this.host = host;
		this.port = port;
		this.farmID = farmID;
		this.dongID = dongID;
		
		input_bytes = new byte[1024];
		send_timer = SimpleTimer.create_inst();
			
		read_word_packet_list = new ArrayList<byte[]>(10);
		for(int i=0; i<10; i++) {
			read_word_packet_list.add(make_read_word_packet((i * 100) + DEFAULT_IDX, 100));
		}
		
		FileUtil.write("INIT => PlcClient Init : " + farmID + dongID);
	}
	
	public void start() {
		
		excutor.execute(new Runnable() {
			
			@Override
			public void run() {
				if(!is_connect()) {
					
					if(!connect()) {
						return;
					}
				}
				
				get_comein_status();
				
				send_timer.stop();
				send_timer.set_runnable(new Runnable() {
					@Override
					public void run() {
						try {
							send_module();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				send_timer.start(100, PERIOD);
			}
		});
		
	}
	
	protected void get_comein_status() {
		
		byte[] send_packet = read_word_packet_list.get(0);
		send(send_packet);
		
		try {
			byte[] read_packet = read();
			
			int start = ((send_packet[8] & 0xFF) << 8) | (send_packet[9] & 0xFF);
			set_sensor_data(start, read_packet);
			
			comein_status = addr_map.getOrDefault(9003, 0) == 1 ? true : false; 
			
			FileUtil.write("TEST => " + farmID + dongID + " comein_status : " + comein_status);
			
		} catch (NullPointerException | IOException e) {
			FileUtil.write("ERROR => IOException in PlcClient get_comein_status " + farmID + dongID);
			delete_inst();
			e.printStackTrace();
		}
	}
	
	protected void send_module() {
		
		if(socket == null || socket.isClosed()) {
			delete_inst();
		}
		
		term += PERIOD;
		
		if(term > 60 * 1000) {		// 1분 주기 실행 - 센서데이터 조회
			
			if(!comein_status) {
				get_comein_status();
			}
			else {
				//comm_buffer.offer(read_word_packet_list.get(0));
				comm_buffer.offer(read_word_packet_list.get(1));
				comm_buffer.offer(read_word_packet_list.get(2));
			}
			
			term -= 60 * 1000;
		}
		
		if(!comein_status) {
			return;
		}
		
		byte[] send_packet = comm_buffer.poll();
		byte[] read_packet;
		
		if(send_packet == null) {		// 이벤트 확인 용 장치 주소 확인 명령
			
			event_read_idx = event_read_idx == 1 ? 3 : event_read_idx;
			
			send_packet = read_word_packet_list.get(event_read_idx);
			
			send(send_packet);
			//FileUtil.write("SEND => " + FileUtil.byte_to_string(send_packet));
			
			try {
				read_packet = read();
				
				if(event_read_idx == 0) {
					check_breed_data(event_read_idx * 100 + DEFAULT_IDX, read_packet);
				}
				else {
					check_event(event_read_idx * 100 + DEFAULT_IDX, read_packet);
				}
				
			} catch (NullPointerException | IOException e) {
				FileUtil.write("ERROR => IOException in PlcClient read " + farmID + dongID);
				delete_inst();
				e.printStackTrace();
			}
			
			event_read_idx = event_read_idx >= 9 ? 0: event_read_idx + 1;
		}
		else {
			send(send_packet);
			
			try {
				read_packet = read();
				
				switch(read_packet[7]) {
				
				case (byte)0x03:
					int start = ((send_packet[8] & 0xFF) << 8) | (send_packet[9] & 0xFF);
					set_sensor_data(start, read_packet);
					
					// 센서데이터 적재
					if(send_packet.equals(read_word_packet_list.get(2))) {
						insert_sensor_data();
					}
					
					break;
				
				case (byte)0x10:
					FileUtil.write("RECV 10 => " + farmID + dongID + " " + FileUtil.byte_to_string(read_packet));
					break;
				
				case (byte)0x90:	// 0x10 에러
					FileUtil.write("RECV 90 => " + farmID + dongID + " " + FileUtil.byte_to_string(read_packet));
					break;
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
		}
		
		//FileUtil.write("SEND => " + FileUtil.byte_to_string(send_packet));
		
	}
	
	protected boolean connect() {
		boolean ret = true;
		
		try {
			socket = new Socket(host, port);
			dis = new DataInputStream(socket.getInputStream());
			dos = new DataOutputStream(socket.getOutputStream());
			
			FileUtil.write("CONNECT => PlcClient Connect : " + farmID + dongID);
			
		} catch (UnknownHostException e) {
			//e.printStackTrace();
			ret = false;
		} catch (IOException e) {
			//e.printStackTrace();
			ret = false;
		}
		
		return ret;
	} 
	
	public boolean is_connect() {
		
		if(socket == null) {
			return false;
		}
		
		return socket.isConnected();
	}
	
	protected boolean send(byte[] packet) {
		if(packet != null && packet.length != 0) {	//sparrow 수정사항
			try {
				dos.write(packet);
				dos.flush();
			} catch (IOException e) {
				FileUtil.write("ERROR => IOException in PlcClient Send " + farmID + dongID);
				delete_inst();
				e.printStackTrace();
				return false;
			}
			
			return true;
		}
		
		return false;
	}
	
	protected byte[] read() throws IOException, NullPointerException{
		
		// DataInputStream이 존재하면 반복
		if (dis != null && dos != null && socket.isConnected()) {
			
			// DataInputStream 값이 있을 때까지 대기
			int input_count = dis.read(input_bytes);
			
			//읽어온 값에 오류가 있으면 강제 종료
			if (input_count == -1) {
				throw new NullPointerException();
			} 
			else {
				// 수신된 모든 바이트를 버퍼에 담음
				byte[] packet = Arrays.copyOf(input_bytes, input_count);
				
				//FileUtil.write("RECV => " + FileUtil.byte_to_string(packet));
				
				return packet;
				
			} // inputCount if else
		} // while
		
		throw new NullPointerException();
	}
	
	protected void check_breed_data(int start_addr, byte[] packet) {
		
		int len = (int) packet[8] & 0xff;
		
		boolean need_update = false;
		
		for(int j=0; j<len; j+=2) {
			int addr = start_addr + (j/2);
			
			if(PlcManager.contains_info(addr)) {
				int val = (packet[9 + j] << 8) | (packet[10 + j] & 0xFF);
				
				addr_map.put(addr, val);
				
				if(addr == 9003) {
					comein_status = val == 1 ? true : false; 
				}
				
				if(addr >= 9021 && addr <= 9024) {
					
					double dv = PlcManager.get_unit_info(addr).get_rule_val(val);
					val = (int) dv;
					
					if(breed_map.get(addr) != val) {		// 변경되었는지 확인하고
						need_update = true;
						breed_map.put(addr, val);		// 변경된 데이터로 업데이트 하도록 넣음
					}
				}
			}
		}
		
		if(need_update && !comein_code.equals("")) {
			
			PlcManager.mysql_executor.execute(new Runnable() {
				
				@Override
				public void run() {
					HashMap<String, String> map = new HashMap<String, String>();
					map.put("cdCode", comein_code);
					map.put("cdDate", DateUtil.get_inst().get_now().substring(0, 10));
					map.put("cdDeath", Integer.toString(breed_map.get(9022)));
					map.put("cdCull", Integer.toString(breed_map.get(9023)));
					map.put("cdThinout", Integer.toString(breed_map.get(9024)));
					map.put("cdInputDate", DateUtil.get_inst().get_now());
					
					MysqlConn.get_sql().upsert("comein_detail", map, Arrays.asList("cdCode", "cdDate"));
					
//					String table = "comein_master AS cm "
//								+ "JOIN ( "
//								+ 		"SELECT cm.*, SUM(cd.cdDeath) AS cdDeath, SUM(cd.cdCull) AS cdCull, SUM(cd.cdThinout) AS cdThinout FROM comein_master AS cm "
//								+ 		"LEFT JOIN comein_detail AS cd ON cd.cdCode = cm.cmCode "
//								+ 		"WHERE cm.cmCode = '" + comein_code + "' GROUP BY cm.cmCode "
//								+ ") AS t ON cm.cmCode = t.cmCode ";
//					
//					map.clear();
//					map.put("cm.cmInsu", Integer.toString(breed_map.get(9021)));
//					map.put("cm.cmDeathCount", "!t.cdDeath");
//					map.put("cm.cmCullCount", "!t.cdCull");
//					map.put("cm.cmThinoutCount", "!t.cdThinout");
//					
//					MysqlConn.get_sql().update(table, "cm.cmCode = '" + comein_code + "'", map);
					
					String select_query = "SELECT cm.*, IFNULL(SUM(cd.cdDeath), 0) AS cdDeath, IFNULL(SUM(cd.cdCull), 0) AS cdCull, IFNULL(SUM(cd.cdThinout), 0) AS cdThinout FROM comein_master AS cm "
							+ "LEFT JOIN comein_detail AS cd ON cd.cdCode = cm.cmCode "
							+ "WHERE cm.cmCode = '" + comein_code + "' GROUP BY cm.cmCode ";
					
					try {
						Statement state = MysqlConn.get_sql().get_statement();
						if(state != null) {
							ResultSet set = state.executeQuery(select_query);
							if(set.next()) {
								int cdDeath = set.getInt("cdDeath");
								int cdCull = set.getInt("cdCull");
								int cdThinout = set.getInt("cdThinout");
								
								map.clear();
								map.put("cmInsu", Integer.toString(breed_map.get(9021)));
								map.put("cmDeathCount", Integer.toString(cdDeath));
								map.put("cmCullCount", Integer.toString(cdCull));
								map.put("cmThinoutCount", Integer.toString(cdThinout));
								
								MysqlConn.get_sql().update("comein_master", "cmCode = '" + comein_code + "'", map);
								
								int data_arr[] = new int[3];
								data_arr[0] = cdDeath;
								data_arr[1] = cdCull;
								data_arr[2] = cdThinout;
								
								input_comm_buffer(make_write_word_packet(9025, data_arr));
								
								FileUtil.write("BREED END => " + farmID + " " + dongID + " -- " + FileUtil.byte_to_string(make_write_word_packet(9022, data_arr)));
							}
							
							set.close();
							state.close();
						}
						
					} catch (SQLException e) {
						e.printStackTrace();
					}
					
					FileUtil.write("BREED => " + farmID + " " + dongID + " -- " + breed_map.toString());
					
				}
			});
		}
	}
	
	protected void check_event(int start_addr, byte[] packet) {
		int len = (int) packet[8] & 0xff;
		
		String now = DateUtil.get_inst().get_now();
		long timestamp = DateUtil.get_inst().get_timestamp(now);
		
		for(int j=0; j<len; j+=2) {
			int addr = start_addr + (j/2);
			
			UnitInfo info = PlcManager.get_unit_info(addr);
			
			if(info != null) {
				int value = ((packet[9 + j]) << 8) | (packet[10 + j] & 0xFF);
				
				if(addr_map.containsKey(addr)) {		// 최초가 아닌경우
					int prev = addr_map.get(addr);
					if(prev != value) {
						Document doc = info.get_event_doc(farmID, dongID, now, timestamp, value);
						DataUpdator.get_inst().event_doc_queue.offer(doc);
						event_send_all(addr, value);
						FileUtil.write("EVENT => " + farmID + " " + dongID + " -- " + addr + " : " + prev + " to " + value);
					}
				}
				else {
					if(value != -1) {
						if(info.get_property().equals("D")) {
							Document doc = info.get_event_doc(farmID, dongID, now, timestamp, value);
							DataUpdator.get_inst().event_doc_queue.offer(doc);
						}
					}
				}
				
				addr_map.put(addr, value);
			}
		}
	}
	
	protected void set_sensor_data(int start_addr, byte[] packet) {
		int len = (int) packet[8] & 0xff;
		
		for(int j=0; j<len; j+=2) {
			int addr = start_addr + (j/2);
			
			if(PlcManager.contains_info(addr)) {
				int val = (packet[9 + j] << 8) | (packet[10 + j] & 0xFF);
				
				addr_map.put(addr, val);
			}
		}
	}
	
	protected void insert_sensor_data() {
		
		try {
			Document doc = new Document();
			String get_time = DateUtil.get_inst().get_now();
			String id = farmID + dongID + "_" + DateUtil.get_inst().get_timestamp(get_time);
			
			doc.append("_id", id);
			doc.append("farmID", farmID);
			doc.append("dongID", dongID);
			doc.append("getTime", get_time);
			
			for(Entry<String, List<Integer>> entry : PlcManager.get_inst().sensor_table_map.entrySet()) {
				String key = entry.getKey();
				List<Integer> list = entry.getValue();
				
				List<String> val_list = new ArrayList<String>();
				for(Integer addr : list) {
					
					UnitInfo info = PlcManager.get_unit_info(addr);
					if(info != null) {
						int val = addr_map.get(addr);
						
						if(val == -1) {
							val_list.add("N");
						}
						else {
							double dbl = info.get_rule_val(addr_map.get(addr));
							val_list.add(Double.toString(dbl));
						}
					}
				}
				
				doc.append(key, val_list);
			}
			
			buffer_update(doc);
			
			FileUtil.write("COMPLETE => insert_sensor_data " + farmID + " " + dongID);
			MongoConn.get_mongo().get_db().getCollection("plcSensor").insertOne(doc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	protected void buffer_update(Document doc) {
		
		try {
			String ipaddr = socket.getInetAddress().getHostAddress().replace("/", "");
			String inout = addr_map.getOrDefault(9003, 0) == 0 ? "O" : "I";
			String days = Integer.toString(addr_map.getOrDefault(9003, 0));
			
			HashMap<String, String> update_map = new HashMap<String, String>();
			update_map.put("bpFarmid", farmID);
			update_map.put("bpDongid", dongID);
			update_map.put("bpIPaddr", ipaddr);
			update_map.put("bpStatus", inout);
			update_map.put("bpDays", days);
			update_map.put("bpSensorDate", DateUtil.get_inst().get_now());
			
			for(String key : PlcManager.get_inst().sensor_table_map.keySet()) {
				String temp = "";
				for(String s : (List<String>) doc.get(key)) {
					temp += s + "|";
				}
				temp = temp.substring(0, temp.length() - 1);
				
				update_map.put("bp" + key, temp);
			}
			
			excutor.execute(new Runnable() {
				
				@Override
				public void run() {
					//FileUtil.write("TEST => " + update_map);
					MysqlConn.get_sql().update("buffer_plc_status", "bpFarmid = '" + farmID + "' AND bpDongid = '" + dongID + "'", update_map);
				}
			});
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void input_comm_buffer(byte[] packet) {
		
		//FileUtil.write("DATA => " + FileUtil.byte_to_string(packet));
		comm_buffer.offer(packet);
		
	}
	
	protected void event_send_all(int addr, int val) {
		for(OperWorker worker : worker_list) {
//			worker.send_event(addr, val);
		}
	}
	
	public void add_workers(OperWorker worker) {
		worker_list.add(worker);
	}
	
	public void remove_workers(OperWorker worker) {
		worker_list.remove(worker);
	}
	
	public void set_breed_map(int addr, int val) {
		breed_map.put(addr, val);
	}
	
	public void set_comein_code(String comein_code) {
		this.comein_code = comein_code;
	}
	
	public void set_addr_map(int addr, int val) {
		addr_map.put(addr, val);
	}
	
	public HashMap<Integer, Integer> get_addr_map(){
		return addr_map;
	}
	
	public void set_passwd(String passwd) {
		this.passwd = passwd;
	}
	
	public String get_passwd() {
		return passwd;
	}
	
	public String get_id() {
		return farmID + dongID;
	}
	
	public void delete_inst() {
		
		if(send_timer != null) {
			send_timer.stop();
		}
		
		if(socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		FileUtil.write("DELETE => " + farmID + dongID + " delete_inst()");
		PlcManager.get_inst().plc_bridge_map.remove(farmID + dongID);
	}
}
