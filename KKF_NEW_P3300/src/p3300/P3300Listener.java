package p3300;

import java.net.Socket;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import module.BufferUpdate;
import module.CallAvgWeight;
import module.CheckComeout;
import module.RequestWorker;
import module.SensorHistoryUpdate;
import server.ServerListener;
import server.ServerWorker;
import util.DateUtil;
import util.FileUtil;
import util.FloatCompute;
import util.MysqlConn;

public class P3300Listener extends ServerListener {
	
	private static P3300Listener instance = null;
	public static P3300Listener get_inst(){
		if(instance == null){
			instance = new P3300Listener();
		}
		
		return instance;
	}
	
	// PHP -> 서버로 명령 전달 후 GW -> 서버로 오는 응답 기다리는 소켓을 저장
	public static ConcurrentHashMap<String, Socket> waiter_map = new ConcurrentHashMap<String, Socket>();
	
	public static ExecutorService camera_executors = Executors.newFixedThreadPool(5);
	public static ExecutorService update_executors = Executors.newFixedThreadPool(5);
	public static ExecutorService mongo_executors = Executors.newSingleThreadExecutor();
	
	ExecutorService executor = null;
	
	// 클라이언트가 연결되었을 때 실행할 작업
	@Override
	protected void create_worker(Socket m_socket){
		if(executor == null){
			executor = Executors.newCachedThreadPool();
		}
		
		executor.execute(new P3300Divider(m_socket));
	}

	// 서버소켓을 바인딩하기 전에 실행할 작업
	@Override
	protected void before_bind() {
		
		long debug = (long) FileUtil.get_config("debug_mode");
		FileUtil.debug_mode = debug == 1 ? true : false;
		
		CallAvgWeight.get_inst().start();		// 평균중량 산출 실행
		CheckComeout.get_inst().start();		// 출하처리
		
		RequestWorker.get_inst().start();		// 재산출 요청 홗인
		BufferUpdate.get_inst();				// 버퍼테이블 업데이트
		SensorHistoryUpdate.get_inst();			// 현재 사용 안함
		
	}
	
	// divider에서 패킷을 분기하고 생성할 worker를 넘겨받아 새로운 쓰레드로 실행
	public void recreate_worker(ServerWorker worker) {
		if(executor == null){
			executor = Executors.newCachedThreadPool();
		}
		
		executor.execute(worker);
	}
}
