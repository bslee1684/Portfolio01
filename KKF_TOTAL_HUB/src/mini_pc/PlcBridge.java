package mini_pc;

import java.util.HashMap;
import java.util.List;

import plc_object.OperWorker;

public interface PlcBridge {
	
	public abstract void start();
	
	public abstract boolean is_connect();
	
	public abstract void add_worker(OperWorker worker);
	
	public abstract void remove_worker(OperWorker worker);
	
	public abstract HashMap<Integer, Integer> get_addr_map();
	
	public abstract HashMap<Integer, Integer> get_bit_map();
	
	public abstract void set_passwd(String passwd);
	
	public abstract String get_passwd();
	
	public abstract String get_id();
	
	public abstract void delete_inst();
	
	public abstract void set_comein_code(String code);
	
	public abstract void set_breed_map(int addr, int val);
	
	public abstract void send_word(int start, List<Integer> int_status, List<Byte> byte_status);
	
	public abstract void send_word(int start, int[] data);
	
	public abstract void send_bit(int start, String status, List<Byte> byte_status);
}
