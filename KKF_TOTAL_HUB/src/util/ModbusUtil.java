package util;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/*
 * 모드버스 프로토콜 모듈
 * 비트 및 워드 제어
 */
public class ModbusUtil {
	
	/*
	 * 필요한 길이의 패킷을 생성
	 * 헤더 추가하여 리턴
	 * - length : 데이터 패킷 길이
	 */
	public byte[] get_base_packet(int length) {
		
		byte[] ret = new byte[7 + length];
		int idx = 0;
		
		ret[idx++] = 0x00;
		ret[idx++] = 0x00;
		ret[idx++] = 0x00;
		ret[idx++] = 0x00;
		
		// 프레임 총 길이 - 7
		String hexlen = String.format("%04x", length);
		ret[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		ret[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		ret[idx++] = 0x00;		
		
		return ret;
		
	}
	
	/*
	 * 워드 읽기 패킷 생성
	 * - start_addr : 시작 주소
	 * - len : 읽을 길이
	 */
	public byte[] make_read_word_packet(int start_addr, int len) {
		
		byte[] data = get_base_packet(5);
		int idx = 7;
		
		data[idx++] = 0x03;
		
		// 시작주소
		String hexstart = String.format("%04x", start_addr);
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
		
		// 조회 길이
		String hexlen = String.format("%04x", len);
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		return data;
		
	}
	
	/*
	 * 멀티 블록 워드 읽기 패킷 생성
	 * - block : 블록별 위치 및 길이
	 * - block --- key : 블록 읽기 시작 주소
	 * - block --- val : 블록 읽기 길이
	 */
	public byte[] make_read_word_multi_block_packet(HashMap<Integer, Integer> block) {
		
		byte[] data = get_base_packet(2 + block.size() * 4);
		int idx = 7;
		
		data[idx++] = 0x65;
		data[idx++] = (byte) block.size();
		
		for(Entry<Integer, Integer> entry : block.entrySet()) {
			Integer addr = entry.getKey();
			Integer len = entry.getValue();
			
			// 시작주소
			String hexstart = String.format("%04x", addr);
			data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
			data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
			
			// 조회 길이
			String hexlen = String.format("%04x", len);
			data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
			data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
			
		}
		
		return data;
		
	}
	
	/*
	 * 워드 쓰기 패킷 생성
	 * - start_addr : 시작 주소
	 * - write_array : 주소에 쓸 데이터 - 시작주소부터 0인덱스
	 */
	public byte[] make_write_word_packet(int start_addr, int[] write_array) {
		
//		byte[] data = new byte[6 + (write_array.length * 2)];
		byte[] data = get_base_packet(6 + (write_array.length * 2));
		int idx = 7;
		
		data[idx++] = 0x10;
		
		// 시작주소
		String hexstart = String.format("%04x", start_addr);
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
		
		// 출력 개수
		String hexlen = String.format("%04x", write_array.length);
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		// 바이트 개수
		data[idx++] = (byte) (write_array.length * 2);		
		
		// 출력 데이터
		for(int out : write_array) {
			String hexout = String.format("%04x", out);
			data[idx++] = (byte) Integer.parseInt(hexout.substring(0, 2), 16);			
			data[idx++] = (byte) Integer.parseInt(hexout.substring(2, 4), 16);	
		}
		
		return data;
	}
	
	/*
	 * 워드 쓰기 패킷 생성
	 * - start_addr : 시작 주소
	 * - write_list : 주소에 쓸 데이터 - 시작주소부터 0인덱스
	 */
	public byte[] make_write_word_packet(int start_addr, List<Integer> write_list) {
		
//		byte[] data = new byte[6 + (write_list.size() * 2)];
		byte[] data = get_base_packet(6 + (write_list.size() * 2));
		int idx = 7;
		
		data[idx++] = 0x10;
		
		// 시작주소
		String hexstart = String.format("%04x", start_addr);
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
		
		// 출력 개수
		String hexlen = String.format("%04x", write_list.size());
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		// 바이트 개수
		data[idx++] = (byte) (write_list.size() * 2);		
		
		// 출력 데이터
		for(Integer out : write_list) {
			String hexout = String.format("%04x", out);
			data[idx++] = (byte) Integer.parseInt(hexout.substring(0, 2), 16);			
			data[idx++] = (byte) Integer.parseInt(hexout.substring(2, 4), 16);	
		}
		
		System.out.println("write word : " + FileUtil.byte_to_string(data));
		
		return data;
		
	}
	
	/*
	 * 비트 읽기 패킷 생성
	 * - start_addr : 시작 주소
	 * - len : 읽을 비트 길이
	 */
	public byte[] make_read_bit_packet(int start_addr, int len) {
		
		byte[] data = get_base_packet(5);
		int idx = 7;
		
		data[idx++] = 0x01;
		
		// 시작주소
		String hexstart = String.format("%04x", start_addr);
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
		
		// 조회 길이
		String hexlen = String.format("%04x", len);
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		return data;
		
	}
	
	/*
	 * 비트 쓰기 패킷 생성
	 * - start_addr : 시작 주소
	 * - status : 비트에 쓸 데이터 (2진 문자열)
	 */
	public byte[] make_write_bit_packet(int start_addr, String status) {
		
		int bytelen = Math.floorDiv(status.length(), 8);
		bytelen += Math.floorMod(status.length(), 8) == 0 ? 0 : 1;
		
		byte[] data = get_base_packet(6 + bytelen);
		int idx = 7;
		
		data[idx++] = 0x0f;
		
		// 시작주소
		String hexstart = String.format("%04x", start_addr);
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexstart.substring(2, 4), 16);	
//		
//		// 비트 개수
		String hexlen = String.format("%04x", status.length());
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(0, 2), 16);			
		data[idx++] = (byte) Integer.parseInt(hexlen.substring(2, 4), 16);	
		
		// 총 바이트 개수 == (비트개수 / 8) + 1
		data[idx++] = (byte) bytelen;
		
		int cnt = 0;
		int write = 0;
		
		for(char c : status.toCharArray()) {
			
			if(c == '1') {
				write += Math.pow(2, cnt);
			}
			
			cnt++;
			if(cnt == 8) {
				data[idx++] = (byte) write;
				
				cnt = 0;
				write = 0;
			}
		}
		
		// 남은 바이트 처리
		if(cnt != 0) {
			data[idx++] = (byte) write;
		}
		
		System.out.println("write word : " + FileUtil.byte_to_string(data));
		
		return data;
		
	}
	
	public String byte_to_bin(byte b) {
		
		int t = b & 0xff;
		
		String ret = String.format("%8s", Integer.toBinaryString(t)).replace(" ", "0");
		
		return ret;
		
	}
	
}
