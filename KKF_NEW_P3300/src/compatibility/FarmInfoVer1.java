package compatibility;


import kokofarm.FarmInfo;
import module.BufferUpdate;
import module.CheckComein;
import util.DateUtil;
import util.FileUtil;

public class FarmInfoVer1 extends FarmInfo{
	
	public static FarmInfo get_inst(String farm, String dong){
		
		if(list_map.containsKey(farm + dong)){
			return list_map.get(farm + dong);
		}
		
		FarmInfo instance = new FarmInfoVer1(farm, dong);
		list_map.put(farm + dong, instance);
		
		return instance;
	}

	protected FarmInfoVer1(String farm, String dong) {
		super(farm, dong);
	}

	@Override
	public void work() {
		
		// 백업 판단 로직
		long last_sensor_stamp = DateUtil.get_inst().get_timestamp(last_sensor_date);
		last_work_stamp = DateUtil.get_inst().get_now_timestamp();
		
		if(!is_backup){		//백업 아닌경우
			if(last_work_stamp - last_sensor_stamp > CHECK_TIME_BACKUP){			// 데이터를 판단 해서 백업이면
				backup_start_date = last_sensor_date;
				
				is_backup = true;
				FileUtil.write("START => Back Up Start " + farm_id + " " + dong_id);
			}
		}
		else {			// 백업인 경우 
			if(last_work_stamp - last_sensor_stamp < 180) {		// 백업 데이터가 다 들어왔으면
				run_backup_end();
			}
		}
	}
	
	public void check() {
		long now_stamp = DateUtil.get_inst().get_now_timestamp();
		if(is_backup) {
			if(now_stamp - last_work_stamp > 240) {
				run_backup_end();
			}
		}
		else {		
			
			//FileUtil.write("CHECK => " + farm_id + dong_id + " work - update : " + (last_work_stamp - last_update_stamp) + " now - work : " + (now_stamp - last_work_stamp));
			
			if(last_work_stamp > last_update_stamp) {
				if((now_stamp - last_work_stamp) > 5) {
					last_update_stamp = now_stamp;							//현재 시간을 최종업데이트 시간으로 기록
					
					BufferUpdate.update_queue.offer(this);
					
					//입추 처리 로직
					if(update_count < CHECK_UPDATE_COUNT) {		//데이터 적재 횟수가 입추 기준 횟수보다 작으면
						update_count++;
						
						if(update_count == 1){
							first_update_date = last_sensor_date;		// 첫 업데이트 시간에 계측 시간 데이터를 기록
						}
						
						//FileUtil.write("TEST => update_count : " + update_count + " last_work_stamp : " + last_work_stamp + " first_update_date " + DateUtil.get_inst().get_timestamp(first_update_date));
						if(update_count == CHECK_UPDATE_COUNT){
							
							if(last_work_stamp - DateUtil.get_inst().get_timestamp(first_update_date) < 780){		// 10번의 데이터가 13분 내에 들어온 경우
								CheckComein.get_inst().work(farm_id, dong_id, first_update_date);
							}
							else{
								update_count = 0;
							}
						}
					}
				}
			}
			else {
				if(now_stamp - last_work_stamp > 240) {
					update_count = 0;		// 시간 경과 시 에러에서 데이터 들어오는것 체크하기 위함
				}
			}
		}
	}
}
