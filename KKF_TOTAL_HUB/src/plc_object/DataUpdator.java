package plc_object;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.Document;

import util.FileUtil;
import util.MongoConn;
import util.SimpleTimer;

public class DataUpdator {
	private static DataUpdator inst = null;
	
	public static DataUpdator get_inst() {
		if(inst == null) {
			inst = new DataUpdator();
		}
		
		return inst;
	}
	
	public ConcurrentLinkedQueue<Document> event_doc_queue = null;
	private SimpleTimer timer = null;
	
	private DataUpdator() {
		FileUtil.write("CREATE => DataUpdator Created");
		
		timer = SimpleTimer.create_inst();
		event_doc_queue = new ConcurrentLinkedQueue<Document>();
	}
	
	public void start() {
		timer.stop();
		
		timer.set_runnable(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					if(!event_doc_queue.isEmpty()) {
						List<Document> mongo_insert_list = new ArrayList<Document>();
						
						while(true) {
							Document doc = event_doc_queue.poll();
							
							if(doc == null) {
								break;
							}
							
							mongo_insert_list.add(doc);
						}
						
						MongoConn.get_mongo().get_db().getCollection("plcHistory").insertMany(mongo_insert_list);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		timer.start(1000, 1000);
	}
}
