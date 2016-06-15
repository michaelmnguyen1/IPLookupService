package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.getcake.capcount.services.CapCountStreamHandler;
import com.getcake.capcount.services.InitCapCountTimerTask_Spark;


public class CakeTimer extends Timer implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = Logger.getLogger(CakeTimer.class);
	
	private List<CakeTimerTask> timerTasks = null;
	private ConcurrentMap<Long, CakeTimer> scheduledTimerMap;
	
	private ConcurrentMap<Long, CakeTimerTask> scheduledTaskMap;
	
	public CakeTimer () {
		timerTasks = new ArrayList<>();
		scheduledTaskMap = new ConcurrentHashMap <> ();
	}
	
	public CakeTimer (ConcurrentMap<Long, CakeTimer> scheduledTimeTimerMap) {
		this.scheduledTimerMap = scheduledTimeTimerMap;
		scheduledTaskMap = new ConcurrentHashMap <> ();
		timerTasks = new ArrayList<>();
	}
	
	public ConcurrentMap<Long, CakeTimerTask> getScheduledTaskMap () {
		return scheduledTaskMap;
	}
	
  	public void scheduleSharedTask(CakeTimerTask timerTask, Date time) {
  		
  		scheduledTaskMap.put(timerTask.scheduledExecutionTime(), timerTask);
  		timerTasks.add(timerTask);  		
  		super.schedule(timerTask, time);  		
  	}
  	
  	public void taskDone (CakeTimerTask timerTask) {
		scheduledTaskMap.remove(timerTask.scheduledExecutionTime()); // intIndex); // 
  		timerTasks.remove(timerTask);  		  		
  	}
  	
  	public int getNumTimerTasks () {
  		return timerTasks.size();
  	}
}
