package run_object;

import util.FileUtil;;

public class RunCamera implements Runnable{
	
	String ip; 
	String port; 
	String image_path;
	String folder;
	String type;
	
	public RunCamera(String m_ip, String m_port, String m_image_path, String m_folder, String m_type) {
		ip = m_ip;
		port = m_port;
		image_path = m_image_path;
		folder = m_folder;
		type = m_type;
	}
	
	public void run() {
		FileUtil.save_url_image(ip, port, image_path, folder, type);
	}
}
