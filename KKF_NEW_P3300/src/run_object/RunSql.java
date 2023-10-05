package run_object;

import java.sql.SQLException;
import java.util.HashMap;

import util.FileUtil;
import util.MysqlConn;

import java.sql.Statement;


public class RunSql implements Runnable{
	
	String table;
	String where;
	HashMap<String, String> map;
	String sql;
	
	String log;
	
	String work = "";
	
	public RunSql(String m_table, String m_where, HashMap<String, String> m_map, String m_log) {
		work = "update";
		table = m_table;
		where = m_where;
		map = m_map;
		log = m_log;
	}
	
	public RunSql(String m_table, HashMap<String, String> m_map, String m_log) {
		work = "insert";
		table = m_table;
		map = m_map;
		log = m_log;
	}
	
	public RunSql(String m_sql, String m_log) {
		work = "direct";
		sql = m_sql;
		log = m_log;
	}
	
	public void run() {
		
		switch (work) {
		case "update":
			MysqlConn.get_sql().update(table, where, map);
			if(!log.equals("")){
				FileUtil.write(log);
			}
			break;

		case "insert":
			MysqlConn.get_sql().insert(table, map);
			if(!log.equals("")){
				FileUtil.write(log);
			}
			break;
			
		case "direct":
			Statement state = MysqlConn.get_sql().get_statement();
			try {
				if(state != null) {
					state.executeUpdate(sql);		//업데이트 수행
				}
				
				if(!log.equals("")){
					FileUtil.write(log);
				}
				
			} catch (SQLException e) {
				//e.printStackTrace();
				FileUtil.write("ERROR => MySql Update SQLException / run_update / " + sql);
			} catch (Exception e) {
				//e.printStackTrace();
				FileUtil.write("ERROR => MySql Update Exception / run_update / " + sql);
			}
			
			try {
				state.close();
			} catch (SQLException e) {
				FileUtil.write("ERROR => MySql Close SQLException / run_update / " + sql);
			}
			
			break;
		}
		
		
	}
}
