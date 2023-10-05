package util;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class HashBuffer {
	
	private ConcurrentHashMap<String, LinkedList<Double>> buffer;
	private int max_size;
	
	public HashBuffer(){
		max_size = 60;
		buffer = new ConcurrentHashMap<String, LinkedList<Double>>();
	}
	
	public HashBuffer(int size){
		max_size = size;
		buffer = new ConcurrentHashMap<String, LinkedList<Double>>();
	}
	
	public void put(String key, double value){
		
		if(!buffer.containsKey(key)){
			buffer.put(key, new LinkedList<Double>());
		}
		
		LinkedList<Double> list = buffer.get(key);
		
		if(list.size() >= max_size){
			list.removeFirst();
		}
		
		list.addLast(value);
	}
	
	public double get_avg_item(String key){
		double avg = 0.0;
		int count = 0;
		
		if(buffer.containsKey(key)){
			LinkedList<Double> list = buffer.get(key);
			
			for(double item : list){
				if(item > -50.0) {
					avg = FloatCompute.plus(avg, item);
					count++;
				}
			}
			
			if(count > 0){
				avg = FloatCompute.divide(avg, count);
			}
			
			list.clear();
		}
		
		return avg;
	}
	
	public double get_sum_item(String key){
		double sum = 0.0;
		
		if(buffer.containsKey(key)){
			LinkedList<Double> list = buffer.get(key);
			
			for(double item : list){
				if(item > -50.0) {
					sum = FloatCompute.plus(sum, item);
				}
			}
			
			list.clear();
		}
		
		return sum;
	}
	
	public void clear_item(String key) {
		LinkedList<Double> list = buffer.get(key);
		
		list.clear();
	}
	
	public boolean containsKey(String key){
		if(buffer.containsKey(key)){
			return true;
		}
		
		return false;
	}
	
	public int size(){
		return buffer.size();
	}
}
