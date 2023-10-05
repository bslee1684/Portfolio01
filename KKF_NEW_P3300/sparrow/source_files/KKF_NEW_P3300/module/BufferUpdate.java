package module;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kokofarm.FarmInfo;
import util.FileUtil;
import util.SimpleTimer;

//********************************************************
//class		buffer_table_data_object
//role		mySql 버퍼테이블 업데이트 데이터 결합, 쿼리문 작성
//			데이터 들어온 시간 저장, 업데이트 실행시간 저장
//call		socket_server_thread
//			socket_server_thread 생성 시 1개 인스턴스 같이 생성됨 
//			my_timer.buffer_tabel_list에 저장됨
//********************************************************
public class BufferUpdate {
	
//	private static final String[] BUFFER_KEY_ARR = new String[]{"beIPaddr", "beSensorDate", "beAvgTemp", "beAvgHumi", "beAvgCo2", "beAvgNh3", "beAvgDust"};
//	private static final String[] SENSOR_KEY_ARR = new String[]{"siSensorDate", "siTemp", "siHumi", "siCo2", "siNh3", "siDust", "siWeight"};
	
	private static BufferUpdate inst = null;
	public static ConcurrentLinkedQueue<FarmInfo> update_queue = new ConcurrentLinkedQueue<FarmInfo>();
	
	private ExecutorService executor;
	
	public static BufferUpdate get_inst(){
		if(inst == null){
			inst = new BufferUpdate();
		}
		
		return inst;
	}
	
	SimpleTimer timer = null;
	
	private BufferUpdate() {
		
		if(executor == null) {
			executor = Executors.newCachedThreadPool();
		}
		
		start();
	}
	
	private void start() {
		if(timer == null) {
			timer = SimpleTimer.create_inst();
			timer.set_runnable(new Runnable() {
				@Override
				public void run() {
					
					//현재 존재하는 모든 FarmInfo를 조사
					for(Entry<String, FarmInfo> entry : FarmInfo.list_map.entrySet()) {
						entry.getValue().check();
					}
					
					List<FarmInfo> update_target_list = new ArrayList<FarmInfo>();
					
					try {
						while(true) {
							FarmInfo info = update_queue.poll();
							
							if(info == null) {
								break;
							}
							
							update_target_list.add(info);
							
							if(update_target_list.size() >= 100) {
								List<FarmInfo> copy_list = new ArrayList<FarmInfo>();
								for(FarmInfo item : update_target_list) {
									copy_list.add(item);
								}
								executor.execute(new BufferMerger(copy_list));
								
								update_target_list.clear();
							}
						}
						
						if(update_target_list.size() > 0){
							executor.execute(new BufferMerger(update_target_list));
						}
						
					} catch (NullPointerException e) {
						FileUtil.write("ERROR => NullPointerException in BufferUpdate timer");
					} catch (IndexOutOfBoundsException e) {
						FileUtil.write("ERROR => IndexOutOfBoundsException in BufferUpdate timer");
					}
					
				}
			});
		}
		
		timer.start(5000, 5000);
	}

}
