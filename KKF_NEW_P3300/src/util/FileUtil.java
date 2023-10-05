package util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//********************************************************
//class		file_io
//role		파일 읽기, 파일 쓰기, json, 로그기록, 설정정보 저장
//********************************************************
public class FileUtil {
	
	public static boolean debug_mode = true;
	private static JSONObject config;
	
	//로그 기록 메소드
	//console 출력 + 파일 출력
	public static void write(String log) {
		String ret = "[" + DateUtil.get_inst().get_now() + "] ";
		ret += log;
		
		if(debug_mode) {
			System.out.println(ret);
		}
		
		ret += "\n";
		write(ret, "./log/3300/" + DateUtil.get_inst().get_now_for_log() + "_log.txt");
	}
	
	public static void write(String log, String file_name) {
		try {
			Path path = Paths.get(file_name);
			Files.createDirectories(path.getParent());
			
			FileChannel fileChannel = FileChannel.open(
				path,
				StandardOpenOption.CREATE,			//파일 생성 옵션
				StandardOpenOption.WRITE,			//파일 쓰기 옵션
				StandardOpenOption.APPEND			//파일 이어쓰기 옵션
			);
			
			Charset charset = Charset.defaultCharset();
			ByteBuffer buffer =  charset.encode(log);
			fileChannel.write(buffer);
				
			fileChannel.close();
			
		}catch(IOException e) {
			System.out.println("ERROR => Config File Read Error / Class : Socket_main");
		}
		catch(Exception e) {
			System.out.println("ERROR => Config File Read Error / Class : Socket_main");
		}
	}
	
	//파일 읽기 메소드
	//파일의 모든 내용을 읽어서 String으로 리턴
	public static String file_read(String file_path){
		String ret = "";
		
		try {
			FileChannel fileChannel = FileChannel.open(
				Paths.get(file_path),
				StandardOpenOption.READ
			);
			
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			
			Charset charset = Charset.defaultCharset();
			int cnt = 0;
				
			while(true) {
				cnt = fileChannel.read(buffer);
				if(cnt == -1) {
					break;
				}
				else {
					buffer.flip();
					ret += charset.decode(buffer).toString();
					buffer.clear();
				}
			}
					
			fileChannel.close();
			
		} catch (IOException e) {
			write("ERROR => " + file_path + " file_read Error / Class : kkf_util");
		} catch (Exception e) {
			write("ERROR => " + file_path + " file_read Error / Class : kkf_util");
		}
		
		return ret;
	}
	
	public static void file_write(String file_path, String contents){
		try {
			Path path = Paths.get(file_path);
			Files.createDirectories(path.getParent());
			Files.deleteIfExists(path);
			
			FileChannel fileChannel = FileChannel.open(
				path,
				StandardOpenOption.CREATE,			//파일 생성 옵션
				StandardOpenOption.WRITE			//파일 쓰기 옵션
			);
			
			Charset charset = Charset.defaultCharset();
			ByteBuffer buffer =  charset.encode(contents);
			fileChannel.write(buffer);
				
			fileChannel.close();
			
		}catch(IOException e) {
			System.out.println("ERROR => File Write IOException / file_io.file_write");
		}
		catch(Exception e) {
			System.out.println("ERROR => File Write Exception / file_io.file_write");
		}
	}
	
	public static boolean file_copy(String source, String target){
		try {
			Path target_path = Paths.get(target);
			Path source_path = Paths.get(source);
			
			if(Files.exists(source_path)){	//복사하려는 대상이 존재하는 경우에만 실행
				
				// 복사 위치 경로에 폴더 생성
		        if (!Files.exists(target_path.getParent())) {
		            Files.createDirectories(target_path.getParent());
		        }
				
				if(Files.isDirectory(source_path)){		//복사하려는 대상이 폴더인 경우
					
					// 복사하려는 대상이 폴더인 경우 폴더를 생성
					if (!Files.exists(target_path)) {
			            Files.createDirectories(target_path);
			        }
					
					Files.list(source_path).forEach(name -> {
						file_copy(name.toString(), target + "/" + name.getFileName());
					});
				}
				else{		//복사하려는 대상이 파일인 경우
					Files.copy(source_path, target_path, StandardCopyOption.REPLACE_EXISTING);
				}
				
			}
			else{
				return false;
			}
			
		}catch(IOException e) {
			write("ERROR => File Copy " + source + " IOException / file_io.file_copy");
			return false;
		}
		catch(Exception e) {
			write("ERROR => File Copy " + source + " IOException / file_io.file_copy");
			return false;
		}
		
		//write("COMPLETE => " + source + " --to-- " + target + " file_copy completed");
		return true;
	}
	
	//파일 삭제
	public static void file_delete(String path, String farmID, String dongID){
		file_delete(path + farmID + "/" + dongID);
	}
	
	public static boolean file_delete(String target){
		try {
			Path path = Paths.get(target);
			
			if(Files.exists(path)){
				if(Files.isDirectory(path)){
					Files.list(path).forEach(name -> {
						file_delete(name.toString());
					});
				}
				
				Files.delete(path);
			}
			
		} catch (Exception e) {
			write("ERROR => " + target + " file_delete Fail");
			
			return false;
		}
		
		//write("COMPLETE => " + target + " file_delete completed");
		return true;
	}
	
	//설정 파일을 읽어와서 저장
	public static void config_setting() {
		//config 정보 불러오기
		String config_path = "ServerSideConfig3300.txt";
				
		try {
			String data = file_read(config_path);
			JSONParser json_parser = new JSONParser();
			config = (JSONObject) json_parser.parse(data);
			
			//System.out.println(data);
				
		}catch(ParseException e) {
			write("ERROR => Config Parsing Error / Class : file_io");
		}catch(Exception e) {
			write("ERROR => Config Parsing Error / Class : file_io");
		}
	}
	
	//설정 파일 제공
	public static Object get_config(String key) {
		return config.get(key);
	}
	
	// 바이트 배열을 16진수 형태의 문자열로 변경
	public static String byte_to_string(byte[] bytes) {
		String rett = "";
		for(byte b : bytes) {
			rett += String.format("%02X", b&0xff);
			rett += " ";
		}
		return rett;
	}
	
	public static void json_modify(String path, int where, String swap){
		
		String data = file_read(path);
		String[] token = data.split("\\n");
		
		token[where] = swap;
		
		String input = "";
		for(int i=0; i<token.length; i++){
			input += token[i] + "\n";
		}
		
		file_write(path, input);
	}
	
	public static strictfp byte[] ascii_to_byte(String ascii) throws StringIndexOutOfBoundsException{
		
		char[] charr = ascii.toCharArray();

		StringBuilder builder = new StringBuilder();

		for (char c : charr) {
			int i = (int) c;
			builder.append(Integer.toHexString(i).toUpperCase());
		}

		String hexString = builder.toString();
		int len = hexString.length();
		byte[] data = new byte[(len / 2) + 2];

		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
					+ Character.digit(hexString.charAt(i + 1), 16));
		}
		data[data.length - 2] = (byte) 0x0D;
		data[data.length - 1] = (byte) 0x0A;

		return data;
	}
	
	public static void save_url_image(String ip, String port, String image_path, String folder, String type) {
		
		String url_string = "http://" + ip + ":" + port + image_path;
		
		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
		
		try {
			//계정 id, pw 입력
			CredentialsProvider credential = new BasicCredentialsProvider();
			credential.setCredentials(new AuthScope(ip, Integer.parseInt(port)), new UsernamePasswordCredentials("admin", "kokofarm5561"));
			
			//credential 적용된 httpClient
			CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credential).build();
			
			HttpPost postRequest = new HttpPost(url_string); //POST 메소드 URL 새성
			HttpResponse response = httpClient.execute(postRequest);
			
			InputStream source = response.getEntity().getContent();
			bis = new BufferedInputStream(source);
			
		} catch (NumberFormatException e) {
			write("ERROR => NumberFormatException / save_url_image " + folder);
		} catch (ClientProtocolException e) {
			write("ERROR => ClientProtocolException / save_url_image " + folder);
		} catch (IllegalStateException e) {
			write("ERROR => IllegalStateException / save_url_image " + folder);
		} catch (IOException e1) {
			write("ERROR => IOException / save_url_image " + folder);
		}
		
		try {
			String file_path = "./capture/" + folder + "/" + DateUtil.get_inst().get_now_simple() + "_" + type + ".jpg";
			String folder_path = "./capture/" + folder;
			
			File folderr = new File(folder_path);
			
			if(!folderr.exists()) {
				folderr.mkdirs();
			}
			
			File file = new File(file_path);
			bos = new BufferedOutputStream(new FileOutputStream(file));
			
			int read = 0;
			while((read = bis.read()) != -1){
				bos.write(read);
			}
			
			write("COMPLETE => Save CCTV Image Complete " + folder);
		} catch (FileNotFoundException e) {
			FileUtil.write("ERROR => FileNotFoundException / save_url_image " + folder);
		} catch (IOException e) {
			FileUtil.write("ERROR => IOException / save_url_image " + folder);
		} catch (NullPointerException e){
			FileUtil.write("ERROR => NullPointerException / save_url_image " + folder);
		}
		
		try {
			if(bis != null){
				bis.close();
			}
			if(bos != null){
				bos.close();
			}
		} catch (IOException e) {
			FileUtil.write("ERROR => buffer stream close error " + folder);
		}
		
//		try {
//			String url_string = "http://" + ip + ":" + port + image_path;
//			//System.out.println(url_string);
//			
//			URL url = new URL(url_string);
//			String file_path = "./capture_camera/" + folder + "/" + my_date.get_inst().get_now_simple() + "_" + type + ".jpg";
//			
//			BufferedImage image = ImageIO.read(url);
//			File file = new File(file_path);
//			
//			if(!file.exists()) {
//				file.mkdirs();
//			}
//			
//			ImageIO.write(image, "jpg", file);
//			
//			write("COMPLETE => Save CCTV Image Complete " + folder);
//		} catch (IOException e) {
//			write("ERROR => Get URL Image Error / file_io " + folder);
//		}
	}
}
