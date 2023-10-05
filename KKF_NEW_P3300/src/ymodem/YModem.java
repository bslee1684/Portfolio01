package ymodem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import util.FileUtil;

public class YModem {
	
	protected static final byte SOH = 0x01; /* Start Of Header */
    protected static final byte STX = 0x02; /* Start Of Text (used like SOH but means 1024 block size) */
    protected static final byte EOT = 0x04; /* End Of Transmission */
    protected static final byte ACK = 0x06; /* ACKnowlege */
    protected static final byte NAK = 0x15; /* Negative AcKnowlege */
    protected static final byte CAN = 0x18; /* CANcel character */

    protected static final byte CPMEOF = 0x1A;
    protected static final byte ST_C = 'C';		//0x43
	
	protected DataInputStream dis;
	protected DataOutputStream dos;
	
	protected byte[] header_block = null;
	protected List<byte[]> block_list = null;
	
	protected CRC16 crc = null;
	
//	public static void main(String[] args) {
//		try {
//			
//			Socket socket = new Socket("192.168.0.70", 22);
//			
//			YModem ym = new YModem(socket.getInputStream(), socket.getOutputStream());
//			ym.read_hex_file("./firmware/qtec_gw.bin");
//			
//			ym.start("./firmware/qtec_gw.bin");
//			
////			System.out.println(ym.header_block.length + " => " + FileUtil.byte_to_string(ym.header_block));
////			
////			int cnt = 0;
////			for(byte[] item : ym.block_list) {
////				System.out.println((cnt++) + " => " + FileUtil.byte_to_string(item));
////			}
//			
////			for(int i=0; i<10; i++) {
////				byte[] item = ym.block_list.get(i);
////				System.out.println(item.length + " => " + FileUtil.byte_to_string(item));
////			}
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public YModem(){
		
		block_list = new ArrayList<byte[]>();
		crc = new CRC16();
	}
	
	public YModem(InputStream inputStream, OutputStream outputStream){
		dis = new DataInputStream(inputStream);
		dos = new DataOutputStream(outputStream);
		
		block_list = new ArrayList<byte[]>();
		crc = new CRC16();
	}
	
	public void start(String file_path) {
		int err = 0;
		
		try {
//			while(err > 5) {
//				byte b = read_byte();
//				
//				System.out.println(b);
//				
//				if(b == ST_C) {
//					break;
//				}
//				else{
//					err++;
//					send_block(0, header_block);
//				}
//			}
			
			// 펌웨어 파일을 읽어옴
			read_hex_file(file_path);
			
			// 파일 헤더 전송
			send_block(0, header_block);
			
			// 
			while(err < 10) {
				byte b = read_byte();
				
				if(b == ST_C) {
					break;
				}
				else{
					err++;
					send_block(0, header_block);
				}
			}
			
			if(err >= 10) throw new IOException("Too many errors caught, abandoning transfer");
			
			// 실제 데이터 전송
			for(int i=0; i<block_list.size(); i++) {
				send_block(i+1, block_list.get(i));
			}
			
			// send eot
			while(err < 10) {
				send_byte(EOT);
				
				byte b = read_byte();
				
				if(b == ACK) {
					break;
				}
				else if(b == CAN) {
					throw new IOException("Transmission terminated");
				}
				else {
					err++;
				}
			}
			
			// send eot
			while(err < 10) {
				send_byte(EOT);
				
				byte b = read_byte();
				
				if(b == ACK) {
					break;
				}
				else if(b == CAN) {
					throw new IOException("Transmission terminated");
				}
				else {
					err++;
				}
			}
			
			// 마지막 empty 전송
			while(err < 10) {
				byte b = read_byte();
				
				if(b == ST_C) {
					send_block(0, new byte[128]);
					break;
				}
				else{
					err++;
				}
			}
			
			if(err >= 10) throw new IOException("Too many errors caught, abandoning transfer");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void read_hex_file(String file_path) throws IOException {
		
		Path path = Paths.get(file_path);
		
		BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        String header = file_path + (char)0 + ((Long) Files.size(path)).toString()+" "+ Long.toOctalString(attr.lastModifiedTime().toMillis() / 1000);
        header_block = Arrays.copyOf(header.getBytes(), 128);
        //header_block = Arrays.copyOf(file_path.getBytes(), 128);
		
		FileChannel fileChannel = FileChannel.open(
			path,
			StandardOpenOption.READ
		);
		
		
		//ByteBuffer buffer = ByteBuffer.allocate(1024);
		int cnt = 0;
		while(true) {
			
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			//ByteBuffer buffer = ByteBuffer.allocate(128);
			cnt = fileChannel.read(buffer);
			if(cnt == -1) {
				break;
			}
			else {
				
				buffer.flip();
				
				byte[] block = buffer.array();
				
				if(cnt <= 128) {
					block = Arrays.copyOf(buffer.array(), 128);
				}
				
				for(int i = cnt; i<block.length; i++) {
					block[i] = CPMEOF;
				}
				
				block_list.add(block);
				
				buffer.clear();
			}
		}
	}
	
	public void read_hex_file_hex(String file_path) throws IOException {
		
		Path path = Paths.get(file_path);
		
		BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
        String header = file_path + (char)0 + ((Long) Files.size(path)).toString()+" "+ Long.toOctalString(attr.lastModifiedTime().toMillis() / 1000);
        header_block = Arrays.copyOf(header.getBytes(), 128);
        
        byte[] data = Files.readAllBytes(path);
        
        for(int i=0; i<data.length; i+=1024) {
        	
        	byte[] block = null;
        	if((data.length - i) > 128) {
        		block = Arrays.copyOfRange(data, i, i + 1024);
        	}
        	else {
        		block = Arrays.copyOfRange(data, i, i + 128);
        	}
        	
        	if(i + 1024 > data.length) {
        		for(int j = data.length - i; i<block.length; j++) {
					block[j] = CPMEOF;
				}
        	}
        	
        	block_list.add(block);
        }
	}
	
	protected void send_block(int number, byte[] block) throws IOException {
		int err = 0;
		
		//System.out.println("SEND => " + FileUtil.byte_to_string(block));
		//System.out.println("SEND => " + number);
		
		byte[] packet = new byte[block.length + 5];

		if(block.length == 1024) {
			packet[0] = STX;
		}
		else {
			packet[0] = SOH;
		}
		
		packet[1] = (byte)number;
		packet[2] = (byte)~number;
		
		for(int n=0; n<block.length; n++) {
			packet[3 + n] = block[n];
		}
		
		byte[] crcbyte = get_crc_bytes(block);
		packet[packet.length - 2] = crcbyte[0];
		packet[packet.length - 1] = crcbyte[1];
		
		FileUtil.write("SEND => " + FileUtil.byte_to_string(packet));
		
		while(err < 20) {
			
			// 구조 => STX or SOH / number / ~number / data(1024 or 128 빈자리는 0x1A) / CRCH / CRCL
			
//			if(block.length == 1024) {
//				dos.write(STX);
//			}
//			else {
//				dos.write(SOH);
//			}
//			
//			dos.write(number);
//			dos.write(~number);
//			dos.write(block);
//			
//			dos.write(get_crc_bytes(block));
			
			dos.write(packet);
			dos.flush();
			
			byte recv = read_byte();
			
			if(recv == ACK) {
				return;
			}
			else if(recv == NAK) {
				dos.write(packet);
				dos.flush();
			}
			else if(recv == CAN) {
				throw new IOException("Transmission terminated");
			}
			else {
				err++;
			}
			
//			try {
//				Thread.sleep(00);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}
		
		throw new IOException("Too many errors caught, abandoning transfer");
	}
	
	protected byte[] get_crc_bytes(byte[] block) {
		byte[] ret = new byte[crc.get_length()];
		
		long crc_val = crc.get_crc(block);
        for (int i = 0; i < crc.get_length(); i++) {
        	ret[crc.get_length() - i - 1] = (byte) ((crc_val >> (8 * i)) & 0xFF);
        }
        
        return ret;
	}
	
	protected byte read_byte() throws IOException {
		if(dis != null) {
			int b = dis.read();
			
			FileUtil.write("RECV => " + FileUtil.byte_to_string(new byte[] {(byte) b}));
			
			return (byte) b;
		}
		else {
			throw new IOException();
		}
	}
	
	protected void send_byte(byte b) throws IOException {
		FileUtil.write("SEND => " + FileUtil.byte_to_string(new byte[] {(byte) b}));
		
		dos.write(b);
		dos.flush();
	}
}
