package server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

import util.FileUtil;

//********************************************************
//class		socket_server
//role		서버소켓 생성, socket accept
//			소켓 연결시 socket_server_thread 생성, 쓰레드 관리		
//call 		socket_main
//********************************************************
public abstract class ServerListner {
	
	// 현재 연결된 소켓 정보 관리
	public static ConcurrentHashMap<String, Socket> socket_map = new ConcurrentHashMap<String, Socket>();
	
	boolean is_bind = false;
	
	protected ServerListner() {								
		FileUtil.write("CREATE => Server Socket Created");
	}
	
	public void start(int server_port, int timeout) {
		
		if(is_bind){
			FileUtil.write("ERROR => ServerListner is already bind");
			return;
		}
		
		before_bind();
		
		// 서버소켓 바인드
		ServerSocket svsk = null;
		
		try {
			svsk = new ServerSocket();				
			svsk.setReuseAddress(false);
			svsk.bind(new InetSocketAddress(server_port));
			is_bind = true;
		}
		catch(IOException e) {
			FileUtil.write("ERROR => Server Socket Bind error");
		}
		catch(Exception e) {
			FileUtil.write("ERROR => Server Socket Bind error");
		}
		
		while(is_bind) {
			// 소켓 연결 대기
			Socket socket = null;
			try {
				socket = svsk.accept();
				socket.setSoTimeout(timeout);		//소켓에 데이터 통신이 3분동안 없을 경우 소켓 종료
				
				create_worker(socket);
				
//				if(executor == null) {
//					executor = Executors.newCachedThreadPool();		//갯수 제한 없는 쓰레드풀
//				}
//				
//				//통합 G/W에서 연결 시 쓰레드생성
//				executor.execute(new ServerWorker(socket));
				
			}catch(SocketException e) {
				FileUtil.write("ERROR => SocketException in socket_server");
			}catch(IOException e) {
				FileUtil.write("ERROR => IOException in socket_server");
			}catch (RejectedExecutionException e) {
				FileUtil.write("ERROR => Socket Open Failed");
				FileUtil.write("ERROR => RejectedExecutionException in socket_server");
				break;
			}catch(Exception e) {
				FileUtil.write("ERROR => Exception in socket_server");
			}
		}
		
		try {
			svsk.close();
		} catch (IOException e) {
			FileUtil.write("ERROR => Program END");
		} catch (Exception e) {
			FileUtil.write("ERROR => Program END");
		}
	}
	
	protected abstract void create_worker(Socket socket);
	
	protected abstract void before_bind();
}
