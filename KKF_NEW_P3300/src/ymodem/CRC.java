package ymodem;

public interface CRC {
	int get_length();
	
	long get_crc(byte[] block);
}
