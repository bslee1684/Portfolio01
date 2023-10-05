package util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimpleTimer {
	public static SimpleTimer create_inst(){
		SimpleTimer instance = new SimpleTimer();
		
		return instance;
	}
	public static SimpleTimer create_inst(Runnable runnable){
		SimpleTimer instance = new SimpleTimer(runnable);
		
		return instance;
	}
	
	ScheduledExecutorService scheduled_excutor;
	Runnable runnable;
	
	private SimpleTimer(){
		
	}
	
	private SimpleTimer(Runnable runnable){
		this.runnable = runnable;
	}
	
	public void set_runnable(Runnable runnable){
		this.runnable = runnable;
	}
	
	public void start(int delay, int rate){
		
		if(runnable != null){
			if(scheduled_excutor != null){
				scheduled_excutor.shutdownNow();
				scheduled_excutor = null;
			}
			
			scheduled_excutor = Executors.newSingleThreadScheduledExecutor();
			scheduled_excutor.scheduleAtFixedRate(runnable, delay, rate, TimeUnit.MILLISECONDS);
		}
	}
	
	public void stop(){
		if(scheduled_excutor != null){
			scheduled_excutor.shutdownNow();
			scheduled_excutor = null;
		}
	}
}
