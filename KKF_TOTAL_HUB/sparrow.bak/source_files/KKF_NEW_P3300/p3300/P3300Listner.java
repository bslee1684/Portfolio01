package p3300;

import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import module.BufferUpdate;
import module.CallAvgWeight;
import module.CheckComeout;
import module.RequestWorker;
import module.SensorHistoryUpdate;
import server.ServerListner;
import util.FileUtil;

public class P3300Listner extends ServerListner {
	
	private static P3300Listner instance = null;
	public static P3300Listner get_inst(){
		if(instance == null){
			instance = new P3300Listner();
		}
		
		return instance;
	}
	
	public static ConcurrentHashMap<String, Socket> waiter_map = new ConcurrentHashMap<String, Socket>();
	
	public static ExecutorService camera_executors = Executors.newSingleThreadExecutor();
	public static ExecutorService update_executors = Executors.newFixedThreadPool(5);
	public static ExecutorService mongo_executors = Executors.newSingleThreadExecutor();
	
	ExecutorService executor = null;
	
	@Override
	protected void create_worker(Socket m_socket){
		if(executor == null){
			executor = Executors.newCachedThreadPool();
		}
		
		executor.execute(new P3300Worker(m_socket));
	}

	@Override
	protected void before_bind() {
		
		long debug = (long) FileUtil.get_config("debug_mode");
		FileUtil.debug_mode = debug == 1 ? true : false;
		
		CallAvgWeight.get_inst().start();
		CheckComeout.get_inst().start();
		
		RequestWorker.get_inst().start();
		BufferUpdate.get_inst();
		SensorHistoryUpdate.get_inst();
	}
}
