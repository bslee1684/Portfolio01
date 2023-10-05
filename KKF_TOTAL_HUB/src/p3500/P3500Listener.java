package p3500;

import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import plc_object.DataUpdator;
import plc_object.PlcManager;
import server.ServerListener;
import server.ServerWorker;
import util.FileUtil;

public class P3500Listener extends ServerListener {
	
	private static P3500Listener instance = null;
	public static P3500Listener get_inst(){
		if(instance == null){
			instance = new P3500Listener();
		}
		
		return instance;
	}
	
	public static ConcurrentHashMap<String, Socket> waiter_map = new ConcurrentHashMap<String, Socket>();
	
	ExecutorService executor = null;
	
	@Override
	protected void create_worker(Socket m_socket){
		if(executor == null){
			executor = Executors.newCachedThreadPool();
		}
		
		executor.execute(new P3500Divider(m_socket));
	}

	@Override
	protected void before_bind() {
		
		PlcManager.get_inst().start();
		DataUpdator.get_inst().start();
	}
	
	public void recreate_worker(ServerWorker worker) {
		if(executor == null){
			executor = Executors.newCachedThreadPool();
		}
		
		executor.execute(worker);
	}
}
