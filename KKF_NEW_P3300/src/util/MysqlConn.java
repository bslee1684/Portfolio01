package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//********************************************************
//class		sql_conn
//role		mySql 커넥션 생성, Statement 제공
//********************************************************
public class MysqlConn {
	private static MysqlConn m = new MysqlConn();	
	
	private String db_name = null;
	private String db_id = null;
	private String db_pw = null;
	private String db_ip = null;
	private String db_port = null;
	private Connection sql_conn = null;
	//private Statement state = null;
	
	//private List<String> firmware_data = new ArrayList<String>();
	
	//private String file_path = "SqlConfig.txt";
	//private String file_path = "src/util/SqlConfig.txt";
	
	private MysqlConn(){
		
	}
	
	public static MysqlConn get_sql() {
		return m;
	}
	
	//DB연결 메소드
	private void conn() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            String url = "jdbc:mysql://" + db_ip + ":" + db_port + "/" + db_name + "?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false";
            
            sql_conn = (Connection) DriverManager.getConnection(url, db_id, db_pw);
            
            //state = (Statement) sql_conn.createStatement();
            
            FileUtil.write("CREATE => SQL Connect Created");
            //System.out.println("[" + my_date.get_inst().get_now() + "] CREATE => SQL Connect Created");
            
        }
        catch(ClassNotFoundException e) {
        	System.out.println("드라이버 로딩 실패");
        	
        }
        catch(SQLException e) {
        	FileUtil.write("ERROR => SQL Connect Failed");
        }
        catch(Exception e) {
        	FileUtil.write("ERROR => SQL Connect Failed");
        }
	}
	
	//시작점
	public void start(String m_name, String m_ip, String m_port, String m_id, String m_pw) {
		db_name = m_name;
		db_ip = m_ip;
		db_port = m_port;
		db_id = m_id;
		db_pw = m_pw;
		
		conn();
		//read_firmware();
	}
	
	//statement 제공
	public Statement get_statement() {
		Statement state = null;
		try {
			state = (Statement) sql_conn.createStatement();
		} catch (SQLException e) {
			FileUtil.write("ERROR => Create Statement Error / Class : sql_conn / func : get_statement");
		} catch (Exception e) {
			FileUtil.write("ERROR => Create Statement Error / Class : sql_conn / func : get_statement");
		}
		return state;
	}
	
	//mysql 수정 메소드
	public void update(String table, String where, HashMap<String, String> map) {
		String update_sql = "UPDATE " + table + " SET ";
		if(map.size() > 0) {
			for(String key : map.keySet()) {
				String val = map.get(key);
				if(val.startsWith("!")) {
					val = val.substring(1);
					update_sql += key + " = " + val + ", ";
				}
				else {
					update_sql += key + " = '" + val + "', ";
				}
			}
			update_sql = update_sql.substring(0, update_sql.length()-2);
			
			update_sql += " WHERE " + where + ";";
			
			Statement state = get_statement();
			
			if(state != null) {
				try {
					state.executeUpdate(update_sql);
				} catch (SQLException e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Update Error " + update_sql + " / Class : sql_conn");
				} catch (Exception e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Update Error " + update_sql + " / Class : sql_conn");
				}
				
				try {
					state.close();
				} catch (SQLException e) {
					FileUtil.write("ERROR => Update Error " + update_sql + " / Class : sql_conn");
				} catch (Exception e) {
					FileUtil.write("ERROR => Update Error " + update_sql + " / Class : sql_conn");
				}
			}
		}
	}
	
	//mysql 입력 메소드
	public void insert(String table, HashMap<String, String> map) {
		String insert_sql = "INSERT INTO " + table + " ";
		String field = "(";
		String value = " VALUES (";
		
		if(map.size() > 0) {
			for(String key : map.keySet()) {
				field += key + ", ";		
				
				String val = map.get(key);
				if(val.startsWith("!")) {
					val = val.substring(1);
					value += val + ", ";
				}
				else {
					value += "'" + val + "', ";
				}
			}
			field = field.substring(0, field.length()-2) + ")";
			value = value.substring(0, value.length()-2) + ")";
			
			Statement state = get_statement();
			
			if(state != null) {
				try {
					state.executeUpdate(insert_sql + field + value);
				} catch (SQLException e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Insert Error " + insert_sql + " / Class : sql_conn");
				} catch (Exception e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Insert Error " + insert_sql + " / Class : sql_conn");
				}
				
				try {
					state.close();
				} catch (SQLException e) {
					FileUtil.write("ERROR => Insert Error " + insert_sql + " / Class : sql_conn");
				} catch (Exception e) {
					FileUtil.write("ERROR => Insert Error " + insert_sql + " / Class : sql_conn");
				}
				
			}
			
		}
		else {
			FileUtil.write("ERROR => Insert No Data " + table + " / Class : sql_conn");
		}
	}
	
	//mysql upsert
	public void upsert(String table, HashMap<String, String> map, List<String> key_list) {
		String upsert_sql = "INSERT INTO " + table + " ";
		String field = "(";
		String value = " VALUES (";
		String dup_sql = "ON DUPLICATE KEY UPDATE ";
		
		if(map.size() > 0) {
			for(String key : map.keySet()) {
				field += key + ", ";		
				
				String val = map.get(key);
				if(val.startsWith("!")) {
					val = val.substring(1);
					value += val + ", ";
					
					if(!key_list.contains(key)) dup_sql += key + " = " + val + ", ";
					
				}
				else {
					value += "'" + val + "', ";
					
					if(!key_list.contains(key)) dup_sql += key + " = '" + val + "', ";
				}
			}
			field = field.substring(0, field.length()-2) + ")";
			value = value.substring(0, value.length()-2) + ")";
			dup_sql = dup_sql.substring(0, dup_sql.length()-2);
			
			Statement state = get_statement();
			
			if(state != null) {
				try {
					state.executeUpdate(upsert_sql + field + value + dup_sql);
				} catch (SQLException e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Insert Error " + upsert_sql + " / Class : sql_conn");
				} catch (Exception e) {
					e.printStackTrace();
					FileUtil.write("ERROR => Insert Error " + upsert_sql + " / Class : sql_conn");
				}
				
				try {
					state.close();
				} catch (SQLException e) {
					FileUtil.write("ERROR => Insert Error " + upsert_sql + " / Class : sql_conn");
				} catch (Exception e) {
					FileUtil.write("ERROR => Insert Error " + upsert_sql + " / Class : sql_conn");
				}
				
			}
			
		}
		else {
			FileUtil.write("ERROR => Insert No Data " + table + " / Class : sql_conn");
		}
	}
	
	//mysql 삭제 메소드
	public void delete(String table, String where) {
		String delete_sql = "DELETE FROM " + table + " WHERE " + where + ";";
		FileUtil.write("DELETE => " + delete_sql);
		
		Statement state = get_statement();
		
		if(state != null) {
			try {
				state.executeUpdate(delete_sql);
			} catch (SQLException e) {
				FileUtil.write("ERROR => Delete Error " + delete_sql + " / Class : sql_conn");
			} catch (Exception e) {
				FileUtil.write("ERROR => Delete Error " + delete_sql + " / Class : sql_conn");
			}
			
			
			try {
				state.close();
			} catch (SQLException e) {
				FileUtil.write("ERROR => Delete Error " + delete_sql + " / Class : sql_conn");
			} catch (Exception e) {
				FileUtil.write("ERROR => Delete Error " + delete_sql + " / Class : sql_conn");
			}
		}
		
		
	}
	
//	public Object select_one(String sql, String field) {
//		//file_io.write(sql);
//		Object ret = null;
//		
//		Statement state = get_statement();
//		if(state != null) {
//			try {
//				ResultSet set = state.executeQuery(sql);
//				
//				if(set.next()) {
//					ret = set.getObject(field);
//				}
//				set.close();
//				
//			} catch (SQLException e) {
//				file_io.write("ERROR => Select one Error " + field + " / Class : sql_conn");
//			} catch (Exception e) {
//				file_io.write("ERROR => Select one Error " + field + " / Class : sql_conn");
//			}
//			
//			try {
//				state.close();
//			} catch (SQLException e) {
//				file_io.write("ERROR => Select one Error " + field + " / Class : sql_conn");
//			} catch (Exception e) {
//				file_io.write("ERROR => Select one Error " + field + " / Class : sql_conn");
//			}
//		}
//		
//		return ret;
//	}
	
	public List<HashMap<String, Object>> select(String sql, String[] fields){
		List<HashMap<String, Object>> ret = new ArrayList<HashMap<String, Object>>();
		Statement state = get_statement();
		ResultSet set = null;
		if(state != null){
			try {
				set = state.executeQuery(sql);
				
				while(set.next()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					for(String field : fields){
						Object temp = set.getObject(field);
						
						temp = temp == null ? "" : temp;
						
						switch (temp.getClass().getName()) {
						case "java.sql.Timestamp":
							temp = set.getString(field);
							break;

						default:
							break;
						}
						row.put(field, temp);
					}
					ret.add(row);
				}
				
			} catch (SQLException e) {
				FileUtil.write("ERROR => SQLException Error " + sql + " / Class : sql_conn");
			} catch (Exception e) {
				e.printStackTrace();
				FileUtil.write("ERROR => Exception Error " + sql + " / Class : sql_conn");
			}
		}
		
		try {
			set.close();
			state.close();
		} catch (SQLException e) {
			FileUtil.write("ERROR => SQLException Error state.close() " + sql + " / Class : sql_conn");
		} catch (Exception e) {
			FileUtil.write("ERROR => Exception Error state.close() " + sql + " / Class : sql_conn");
		}
		
		return ret;
	}
	
	
}
