package p3300;

import util.FileUtil;
//import module.generator;
import util.MongoConn;
import util.MysqlConn;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import org.slf4j.LoggerFactory;

public class P3300Main {
	public static void main(String args[]) {
//		
		//몽고 DB 로그 출력 X
		((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.WARN);
		
		//설정 파일 불러오기
		FileUtil.config_setting();
		
		//kkf_util.write(data);
		
		//-----------------------------
		//몽고 DB 커넥트
		//mg_conn 인스턴스 호출
		//-----------------------------
		MongoConn.get_mongo().start(
				(String) FileUtil.get_config("mongo_admin"),
				(String) FileUtil.get_config("mongo_ip"),
				(String) FileUtil.get_config("mongo_port"),
				(String) FileUtil.get_config("mongo_id"),
				(String) FileUtil.get_config("mongo_pw"),
				(String) FileUtil.get_config("mongo_db_name")
			);
		
		//-----------------------------
		//MYSQL 커넥트
		//sql_conn 인스턴스 호출
		//-----------------------------
		MysqlConn.get_sql().start(
				(String) FileUtil.get_config("sql_name"),
				(String) FileUtil.get_config("sql_ip"),
				(String) FileUtil.get_config("sql_port"),
				(String) FileUtil.get_config("sql_id"),
				(String) FileUtil.get_config("sql_pw")
			);
		
		//-----------------------------
		//서버 시작
		//socket_server 인스턴스 호출
		//-----------------------------
		int port = (int) (long)FileUtil.get_config("server_port");
		P3300Listner server = P3300Listner.get_inst();
		server.start(port, 180000);
		
	}
}
