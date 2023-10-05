package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

//********************************************************
//class		mg_conn
//role		몽고 db 커넥션, 커넥션 제공, aggregate 명령 제공
//********************************************************
public class MongoConn{
	private static MongoConn mongo = new MongoConn();	//싱글턴 인스턴스
	
	//private static mg_conn mongo_for_aggregate = new mg_conn();		//aggregate을 위한 단독 커넥션
	
	private MongoClient mongo_client = null;
	private String mg_ip = null;
	private String mg_port = null;
	private String mg_admin = null;
	private String mg_db_name = null;
	private String mg_id = null;
	private String mg_pw = null;
	
	private MongoDatabase mg_db = null;
	
	//private String file_path = "MongoConfig.txt";
	//private String file_path = "src/util/MongoConfig.txt";
	
	private Document group_stage_query = null;				//avgweight 그룹 스테이지 쿼리
	private Document project_stage_query = null;			//aggregate 프로젝트 스테이지 쿼리
	
	public static MongoConn get_mongo() {			//싱글턴 인스턴스 리턴
		return mongo;
	}
	
	public MongoDatabase get_db() {							//데이터베이스를 직접 리턴
		return mg_db;
	}
	
//	public MongoCollection<Document> get_coll() {			//인스턴스 연결 컬렉션 리턴
//		return mg_coll;
//	}
	
	public void start(String m_admin, String m_ip, String m_port, String m_id, String m_pw, String m_db_name) {
		mg_admin = m_admin;
		mg_ip = m_ip;
		mg_port = m_port;
		mg_id = m_id;
		mg_pw = m_pw;
		mg_db_name = m_db_name;
		
		try {
			if(mg_pw != null) {
				
				MongoCredential credential = MongoCredential.createScramSha1Credential(mg_id, mg_admin, mg_pw.toCharArray());		//몽고 db 계정정보
				//mongo_client = new MongoClient(new ServerAddress(mg_ip + ":" + mg_port), Arrays.asList(credential));					//몽고 db 보안접속
				mongo_client = new MongoClient(new ServerAddress(mg_ip + ":" + mg_port), credential, new MongoClientOptions.Builder().build());
				
				FileUtil.write("CREATE => Mongo Connect Created");
				
				mg_db = mongo_client.getDatabase(mg_db_name);		//데이터 베이스 불러오기
				
				aggregate_init();	//aggregate 초기 쿼리값을 설정
			}
			else {
				FileUtil.write("ERROR => Mongo Connect Failed");
			}
			
		}catch(MongoException e) {
			//e.printStackTrace();
			FileUtil.write("ERROR => Mongo Connect Failed");
		}catch (Exception e) {
			//e.printStackTrace();
			FileUtil.write("ERROR => Mongo Connect Failed");
		}
		
	}
	
	//----------------------------------------------------------------------------------------
	// Aggregate
	// Comm		Aggregate 목적
	// start	검색 데이터 시작시간
	// end		검색 데이터 마지막 시간
	//----------------------------------------------------------------------------------------
	public AggregateIterable<Document> aggregate_for_avg(String comm, String start, String end){
		return aggregate_for_avg(comm, start, end, "", "");
	}
	
	public AggregateIterable<Document> aggregate_for_avg(String comm, String start, String end, String farmID, String dongID){
		
		AggregateIterable<Document> result = null;
		
		try {
			List<Document> pipeline = null;
			
			switch(comm) {
				case "avg_weight":		//정상 평균중량 산출 쿼리
					pipeline = Arrays.asList(			//aggregation 파이프라인 구성
						new Document()
						.append("$match", new Document()
							.append("getTime", new Document().append("$gte", start).append("$lte", end))		//getTime이 start ~ end 사이인 데이터를 모두 가져옴
							//.append("jeoulID", new Document().append("$lte", "12"))		// 하층부 센서만 가져와서 산출
						),
						
						new Document()
						.append("$sort", new Document()															//getTime으로 오름차순 정렬
							.append("getTime", 1)
						),
						
						new Document()
						.append("$group", group_stage_query),													//w01, w02, ...., w60 필드를 배열로 통합
						
						new Document()
						.append("$project", project_stage_query),												//통합된 필드배열을 하나의 배열로 통합 ConcatArrays 사용
						
						new Document()
						.append("$sort", new Document()															//통합된 데이터를 _id(farmID, dongID)로 내림차순 정렬
							.append("_id", -1)
						),
						
						//개별저울 방식일 시 추가
						new Document()
						.append("$group", new Document()
							.append("_id", new Document().append("farmID", "$_id.farmID").append("dongID", "$_id.dongID"))
							.append("data", new Document().append("$push", new Document()
									.append("jeoulID", "$_id.jeoulID").append("data", "$data")
							))
						)
					);
					
					break;
					
				case "recalculate":		//백업 데이터 재산출 평균중량 쿼리
					pipeline = Arrays.asList(			//aggregation 파이프라인 구성
						new Document()
						.append("$match", new Document()
							.append("farmID", farmID)
							.append("dongID", dongID)
							.append("getTime", new Document().append("$gte", start).append("$lt", end))		//getTime이 start ~ end 사이인 데이터를 모두 가져옴
						),
						
						new Document()
						.append("$sort", new Document()															//getTime으로 오름차순 정렬
							.append("getTime", 1)
						),
							
						new Document()
						.append("$group", get_recalculate_group_stage_query(start, end)),													//w01, w02, ...., w60 필드를 배열로 통합
						
						new Document()
						.append("$project", project_stage_query),												//통합된 필드배열을 하나의 배열로 통합 ConcatArrays 사용
							
						new Document()
						.append("$sort", new Document()															//통합된 데이터를 _id(farmID, dongID)로 내림차순 정렬
							.append("_id", 1)
						),
						
						//개별저울 방식일 시 추가
						new Document()
						.append("$group", new Document()
							.append("_id", "$_id.getTime")
							.append("data", new Document().append("$push", new Document()
									.append("jeoulID", "$_id.jeoulID").append("data", "$data")
							))
						)
					);
					
					break;
			}
			
			//몽고 Aggregate 쿼리 값 출력
//			String test = "";
//			for(int i=0; i<pipeline.size(); i++) {
//				test += pipeline.get(i).toJson();
//				if(i != pipeline.size()-1) {
//					test += ", ";
//				}
//			}
//			//test += pipeline.get(0).toJson();
//			file_io.write(test);
			
			result = mg_db.getCollection("sensorData").aggregate(pipeline).allowDiskUse(true);				//생성한 파이프라인을 aggregate 하여 리턴 
			
		}catch (MongoException e) {
			FileUtil.write("ERROR => Aggregate Error " + comm + " / Class : mg_conn");
		}catch (Exception e) {
			FileUtil.write("ERROR => Aggregate Error " + comm + " / Class : mg_conn");
		}
		
		return result;
	}
	
	//group stage, project stage 쿼리 초기 설정
	private void aggregate_init() {
		//---------------------------------------------------------------------------------------------------------
		// group stage, project stage 쿼리
		//---------------------------------------------------------------------------------------------------------
		try {
			List<String> field_arr = new ArrayList<String>();
			
			group_stage_query = new Document();
			//group_stage_query.append("_id", new Document().append("farmID", "$farmID").append("dongID", "$dongID"));										//통합저울 방식
			group_stage_query.append("_id", new Document().append("farmID", "$farmID").append("dongID", "$dongID").append("jeoulID", "$jeoulID"));			//개별저울 방식
				
			for(int w = 1; w <= 60; w++) {																						//w01, w02, ..., w60 필드를 그룹화 하여 배열로 변경
				group_stage_query.append(String.format("%d", w), new Document().append("$push", String.format("$w%02d", w)));
				field_arr.add(String.format("$%d", w));																			//project stage 쿼리에서 사용할 필드 이름을 구성
			}
			
			//---------------------------------------------------------------------------------------------------------
			// project stage 쿼리
			//---------------------------------------------------------------------------------------------------------
			project_stage_query = new Document();
			
			project_stage_query.append("_id", 1);																				
			project_stage_query.append("data", new Document().append("$concatArrays", field_arr));								//데이터를 하나의 필드로 묶음
				
		}catch(MongoException e) {
			FileUtil.write("ERROR => Aggregate init Fail");
		}catch(Exception e) {
			FileUtil.write("ERROR => Aggregate init Fail");
		}
	}
	
	public AggregateIterable<Document> aggregate_with_option(String m_coll_name, String comm, String farmID, String dongID){
		return aggregate_with_option(m_coll_name, comm, farmID, dongID, "", "");
	}
	public AggregateIterable<Document> aggregate_with_option(String m_coll_name, String comm, String farmID, String dongID, String start, String end){
		AggregateIterable<Document> result = null;
		
		try {
			List<Document> pipeline = null;
			
			switch (comm) {
			case "lastOne":
				pipeline = Arrays.asList(					//aggregation 파이프라인 구성
					new Document()
					.append("$match", new Document()
						.append("farmID", farmID)
						.append("dongID", dongID)
					),
								
					new Document()
					.append("$sort", new Document()			//getTime으로 오름차순 정렬
							.append("getTime", -1)
					),
							
					new Document()
						.append("$limit", 1)
						
				);
				break;

			case "comein":
				pipeline = Arrays.asList(					//aggregation 파이프라인 구성
					new Document()
					.append("$match", new Document()
						.append("farmID", farmID)
						.append("dongID", dongID)
						.append("getTime", new Document()
								.append("$gte", start)
								.append("$lte", end)
							)
					),
									
					new Document()
					.append("$limit", 1)
								
				);
				break;
			}
			
			result = mg_db.getCollection(m_coll_name).aggregate(pipeline).allowDiskUse(true);				//생성한 파이프라인을 aggregate 하여 리턴 
			
			//몽고 Aggregate 쿼리 값 출력
//			String test = "";
//			for(int i=0; i<pipeline.size(); i++) {
//				test += pipeline.get(i).toJson();
//				if(i != pipeline.size()-1) {
//					test += ", ";
//				}
//			}
//			//test += pipeline.get(0).toJson();
//			file_io.write(test);
		}
		catch (MongoException e) {
			FileUtil.write("ERROR => Aggregate Other " + comm + " / Class : mg_conn");
		}catch (Exception e) {
			FileUtil.write("ERROR => Aggregate Other " + comm + " / Class : mg_conn");
		}
		
		return result;
	}
	
	//재산출 시 데이터 호출 코드
	private Document get_recalculate_group_stage_query(String start, String end) {
		Document ret = new Document();
		try{
			List<Document> branches = new ArrayList<Document>();
			while(!start.equals(end)) {
				String tp_end = start.substring(0, 15) + "9:59";
				Document cases = new Document();
				
				List<Document> and_list = new ArrayList<Document>();
				List<String> gte_list = new ArrayList<String>();
				List<String> lte_list = new ArrayList<String>();
				
				gte_list.add("$getTime");	gte_list.add(start);
				lte_list.add("$getTime");	lte_list.add(tp_end);
				
				and_list.add(new Document().append("$gte", gte_list));
				and_list.add(new Document().append("$lte", lte_list));
				
				cases.append("case", new Document().append("$and", and_list));
				cases.append("then", start);
				start = DateUtil.get_inst().get_plus_minus_minute_time(start, 10);		//시작 시간에 10분을 더함
				branches.add(cases);
			}
			
			//통합 저울 산출
			//ret.append("_id", new Document().append("$switch", new Document().append("branches", branches).append("default", "err")));
			
			//개별 저울 산출 수정
			ret.append("_id", new Document()
					.append("getTime", new Document().append("$switch", new Document().append("branches", branches).append("default", "err")))
					.append("jeoulID", "$jeoulID")
			);
			
			for(int w = 1; w <= 60; w++) {																						//w01, w02, ..., w60 필드를 그룹화 하여 배열로 변경
				ret.append(String.format("%d", w), new Document().append("$push", String.format("$w%02d", w)));
			}
			
		}catch(MongoException e) {
			FileUtil.write("ERROR => Aggregate get_recalculate_group_stage_query Fail");
		}catch(Exception e) {
			FileUtil.write("ERROR => Aggregate get_recalculate_group_stage_query Fail");
		}
		
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	public HashMap<String, List<Document>> get_term_data(String start, String end, String farmID, String dongID){
		HashMap<String, List<Document>> ret = new HashMap<String, List<Document>>();
		
		//하루 단위로 시간 분리
		int total_days = DateUtil.get_inst().get_total_days(start, end);
		String temp_start = start;
		String temp_end = "";
		
		//백업 데이터 재산출 대상 데이터 불러오기
		AggregateIterable<Document> output = null;
		
		for(int i=1; i<=total_days; i++) {				//재산출 데이터를 날짜별로 잘라서 재산출함
			
			if(i == total_days) {		//시작 시간과 종료시간의 날짜가 같을 때
				temp_end = end;
			}
			else {						//시작 시간과 종료시간의 날짜가 다를 때
				temp_end = DateUtil.get_inst().get_midNight(temp_start);
			}
			
			FileUtil.write("AGGREGATE => " + temp_start + " ~ " + temp_end);
			
			// 2021-02-22 이병선 재산출 마지막 시간이 00:00:00초인 경우를 방지- exception 발생함 
			if(temp_start.equals(temp_end)){
				break;
			}
			output = aggregate_for_avg("recalculate", temp_start, temp_end, farmID, dongID);
			
			if(output != null) {
				//aggregate 데이터 파싱
				MongoCursor<Document> it = output.iterator();
				
				while(it.hasNext()) {
					
					Document row = it.next();
					String getTime = row.getString("_id");
					
					if(getTime.equals("err")) {
						continue;
					}
					
					ret.put(getTime, (List<Document>) row.get("data"));
					
				} // while
			}
			
			temp_start = temp_end;		//다음 for문에서 사용
			
		}// for
		
		return ret;
	}
}



// 평균중량 호출을 위한 중량 가져오기
//					db.generate.aggregate([
//                       { 
//                           "$match" : { 
//                               "getTime" : { "$gte" : "2020-01-14 16:55:00", "$lte" : "2020-01-14 17:05:00" }
//                           }
//                       },
//                       {   
//                           '$sort' : { 'getTime' : 1 }
//                       },
//                       {
//                           "$group" : {
//                               "_id" : { "farmID" : "$farmID", "dongID" : "$dongID" },
//                               "1" : { "$push" : "$w01" },
//                               "2" : { "$push" : "$w02" },
//                               "3" : { "$push" : "$w03" },
//                               "4" : { "$push" : "$w04" },
//                               "5" : { "$push" : "$w05" },
//                               "6" : { "$push" : "$w06" },
//                               "7" : { "$push" : "$w07" },
//                               "8" : { "$push" : "$w08" },
//                               "9" : { "$push" : "$w09" },
//                               "10" : { "$push" : "$w10" },
//                               "11" : { "$push" : "$w11" },
//                               "12" : { "$push" : "$w12" },
//                               "13" : { "$push" : "$w13" },
//                               "14" : { "$push" : "$w14" },
//                               "15" : { "$push" : "$w15" },
//                               "16" : { "$push" : "$w16" },
//                               "17" : { "$push" : "$w17" },
//                               "18" : { "$push" : "$w18" },
//                               "19" : { "$push" : "$w19" },
//                               "20" : { "$push" : "$w20" },
//                               "21" : { "$push" : "$w21" },
//                               "22" : { "$push" : "$w22" },
//                               "23" : { "$push" : "$w23" },
//                               "24" : { "$push" : "$w24" },
//                               "25" : { "$push" : "$w25" },
//                               "26" : { "$push" : "$w26" },
//                               "27" : { "$push" : "$w27" },
//                               "28" : { "$push" : "$w28" },
//                               "29" : { "$push" : "$w29" },
//                               "30" : { "$push" : "$w30" },
//                               "31" : { "$push" : "$w31" },
//                               "32" : { "$push" : "$w32" },
//                               "33" : { "$push" : "$w33" },
//                               "34" : { "$push" : "$w34" },
//                               "35" : { "$push" : "$w35" },
//                               "36" : { "$push" : "$w36" },
//                               "37" : { "$push" : "$w37" },
//                               "38" : { "$push" : "$w38" },
//                               "39" : { "$push" : "$w39" },
//                               "40" : { "$push" : "$w40" },
//                               "41" : { "$push" : "$w41" },
//                               "42" : { "$push" : "$w42" },
//                               "43" : { "$push" : "$w43" },
//                               "44" : { "$push" : "$w44" },
//                               "45" : { "$push" : "$w45" },
//                               "46" : { "$push" : "$w46" },
//                               "47" : { "$push" : "$w47" },
//                               "48" : { "$push" : "$w48" },
//                               "49" : { "$push" : "$w49" },
//                               "50" : { "$push" : "$w50" },
//                               "51" : { "$push" : "$w51" },
//                               "52" : { "$push" : "$w52" },
//                               "53" : { "$push" : "$w53" },
//                               "54" : { "$push" : "$w54" },
//                               "55" : { "$push" : "$w55" },
//                               "56" : { "$push" : "$w56" },
//                               "57" : { "$push" : "$w57" },
//                               "58" : { "$push" : "$w58" },
//                               "59" : { "$push" : "$w59" },
//                               "60" : { "$push" : "$w60" },
//                           }
//                       },
//                       {
//                           "$project" : {
//                               "_id" : 1,
//                               "data" : {
//                                   "$concatArrays" : [
//                                       "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", 
//                                       "$11", "$12", "$13", "$14", "$15", "$16", "$17", "$18", "$19", "$20",
//                                       "$21", "$22", "$23", "$24", "$25", "$26", "$27", "$28", "$29", "$30", 
//                                       "$31", "$32", "$33", "$34", "$35", "$36", "$37", "$38", "$39", "$40", 
//                                       "$41", "$42", "$43", "$44", "$45", "$46", "$47", "$48", "$49", "$50", 
//                                       "$51", "$52", "$53", "$54", "$55", "$56", "$57", "$58", "$59", "$60", 
//                                       ]
//                               }
//                           }
//                       },
//                       {   
//                           '$sort' : { '_id' : 1 }
//                       }
//                   ])



// 입추 판단
//db.testdata.aggregate([
//                       {
//                           '$match' : {
//                               'getTime' : {'$gte' : "2020-07-03 15:24:00", '$lte' : '2020-07-03 15:34:00'}
//                           }
//                       }, 
//                       {
//                           '$group': { 
//                               '_id' : {'farmID' : '$farmID', 'dongID' : '$dongID', 'jeoulID' : '$jeoulID'},
//                               'count' : {'$sum' : 1},
//                           }
//                       },
//                       {
//                           '$group': { 
//                               '_id' : {'$concat': ["$_id.farmID", "$_id.dongID"]},
//                               'max' : {'$max' : "$count"},
//                           }
//                       },
//                       {
//                           '$sort' : {'_id' : 1}
//                       },
//                       {
//                           '$limit' : 100
//                       }
//                   ])


// 평균중량 재산출
//db.testdata.aggregate([
//                       { 
//                           "$match" : { 
//                               "farmID" : "KF0101", 
//                               "dongID" : "01",
//                               "getTime" : { "$gte" : "2020-07-09 00:00:00", "$lte" : "2020-07-09 23:00:00" }
//                           }
//                       },
//                       {   
//                           '$sort' : { 'getTime' : 1 }
//                       },
//                       {
//                           "$group" : {
//                               "_id" : { 
//                                    $switch: {
//                                        branches: [
//                                            { case: { "$and" : [ 
//                                                {"$gte" : ["$getTime", "2020-07-09 12:20:00"]}, 
//                                                {"$lte" : ["$getTime", "2020-07-09 12:29:59"]}
//                                            ] }, then : "2020-07-09 12:20:00"},
//                                            { case: { "$and" : [ 
//                                                {"$gte" : ["$getTime", "2020-07-09 12:30:00"]}, 
//                                                {"$lte" : ["$getTime", "2020-07-09 12:39:59"]}
//                                            ] }, then : "2020-07-09 12:30:00"},
//                                            { case: { "$and" : [ 
//                                                {"$gte" : ["$getTime", "2020-07-09 12:40:00"]}, 
//                                                {"$lte" : ["$getTime", "2020-07-09 12:49:59"]}
//                                            ] }, then : "2020-07-09 12:40:00"}
//                                        ],
//                                        default: "default value"
//                                    }   
//                               },
//                               "1" : { "$push" : "$w01" },
//                               "2" : { "$push" : "$w02" },
//                               "3" : { "$push" : "$w03" },
//                               "4" : { "$push" : "$w04" },
//                               "5" : { "$push" : "$w05" },
//                               "6" : { "$push" : "$w06" },
//                               "7" : { "$push" : "$w07" },
//                               "8" : { "$push" : "$w08" },
//                               "9" : { "$push" : "$w09" },
//                               "10" : { "$push" : "$w10" },
//                               "11" : { "$push" : "$w11" },
//                               "12" : { "$push" : "$w12" },
//                               "13" : { "$push" : "$w13" },
//                               "14" : { "$push" : "$w14" },
//                               "15" : { "$push" : "$w15" },
//                               "16" : { "$push" : "$w16" },
//                               "17" : { "$push" : "$w17" },
//                               "18" : { "$push" : "$w18" },
//                               "19" : { "$push" : "$w19" },
//                               "20" : { "$push" : "$w20" },
//                               "21" : { "$push" : "$w21" },
//                               "22" : { "$push" : "$w22" },
//                               "23" : { "$push" : "$w23" },
//                               "24" : { "$push" : "$w24" },
//                               "25" : { "$push" : "$w25" },
//                               "26" : { "$push" : "$w26" },
//                               "27" : { "$push" : "$w27" },
//                               "28" : { "$push" : "$w28" },
//                               "29" : { "$push" : "$w29" },
//                               "30" : { "$push" : "$w30" },
//                               "31" : { "$push" : "$w31" },
//                               "32" : { "$push" : "$w32" },
//                               "33" : { "$push" : "$w33" },
//                               "34" : { "$push" : "$w34" },
//                               "35" : { "$push" : "$w35" },
//                               "36" : { "$push" : "$w36" },
//                               "37" : { "$push" : "$w37" },
//                               "38" : { "$push" : "$w38" },
//                               "39" : { "$push" : "$w39" },
//                               "40" : { "$push" : "$w40" },
//                               "41" : { "$push" : "$w41" },
//                               "42" : { "$push" : "$w42" },
//                               "43" : { "$push" : "$w43" },
//                               "44" : { "$push" : "$w44" },
//                               "45" : { "$push" : "$w45" },
//                               "46" : { "$push" : "$w46" },
//                               "47" : { "$push" : "$w47" },
//                               "48" : { "$push" : "$w48" },
//                               "49" : { "$push" : "$w49" },
//                               "50" : { "$push" : "$w50" },
//                               "51" : { "$push" : "$w51" },
//                               "52" : { "$push" : "$w52" },
//                               "53" : { "$push" : "$w53" },
//                               "54" : { "$push" : "$w54" },
//                               "55" : { "$push" : "$w55" },
//                               "56" : { "$push" : "$w56" },
//                               "57" : { "$push" : "$w57" },
//                               "58" : { "$push" : "$w58" },
//                               "59" : { "$push" : "$w59" },
//                               "60" : { "$push" : "$w60" },
//                           }
//                       },
//                       {
//                           "$project" : {
//                               "_id" : 1,
//                               "data" : {
//                                   "$concatArrays" : [
//                                       "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", 
//                                       "$11", "$12", "$13", "$14", "$15", "$16", "$17", "$18", "$19", "$20",
//                                       "$21", "$22", "$23", "$24", "$25", "$26", "$27", "$28", "$29", "$30", 
//                                       "$31", "$32", "$33", "$34", "$35", "$36", "$37", "$38", "$39", "$40", 
//                                       "$41", "$42", "$43", "$44", "$45", "$46", "$47", "$48", "$49", "$50", 
//                                       "$51", "$52", "$53", "$54", "$55", "$56", "$57", "$58", "$59", "$60", 
//                                       ]
//                               }
//                           }
//                       },
//                       {   
//                           '$sort' : { '_id' : 1 }
//                       }
//                   ])
