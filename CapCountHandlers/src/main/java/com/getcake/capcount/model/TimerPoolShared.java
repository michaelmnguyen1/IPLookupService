package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

public class TimerPoolShared implements Serializable {
	private static final Logger logger = Logger.getLogger(TimerPoolShared.class);
	private static TimerPoolShared instance;
	
	private static long TIMER_BUFFER_DUR_MILLISECONDS = 60000;
	
	private ConcurrentMap<Long, CakeTimer> scheduledTimeTimerMap;
	
    private int currTimerIndex = -1;
	private List<CakeTimer> timerTasks = null;
		
	static public TimerPoolShared init (int poolSize) {
		CakeTimer timer;
		
		if (instance != null) {
			return instance;
		}

		synchronized (TimerPoolShared.class) {
			if (instance != null) {
				return instance;
			}
			
			instance = new TimerPoolShared(poolSize);		  
			logger.debug("TimerPool - init poolSize: " + poolSize);
			instance.timerTasks = new ArrayList<>(poolSize);
			
			for (int i = 0; i < poolSize; i++) {
				timer = new CakeTimer (instance.scheduledTimeTimerMap);
				instance.timerTasks.add(timer);
			}			
		}
		return instance;		
	}
	
	private TimerPoolShared (int poolSize) {	
		scheduledTimeTimerMap = new ConcurrentHashMap<> (poolSize);
	}
	
	public static TimerPoolShared getInstance() {
		return instance;
	}	
	
	synchronized public CakeTimer getTimer (long scheduledTime) {
		CakeTimer cakeTimer, nextTimer;
		Date currDate;
		
		currDate = Calendar.getInstance().getTime();
		if ((scheduledTime - currDate.getTime()) <= TIMER_BUFFER_DUR_MILLISECONDS) {
			if (++currTimerIndex == timerTasks.size()) {
				currTimerIndex = 0;
			}
			logger.debug("(scheduledTime - currDate.getTime()) <= TIMER_BUFFER_DUR_MILLISECONDS) using timer at index " + currTimerIndex);
			nextTimer = timerTasks.get(currTimerIndex);
			scheduledTimeTimerMap.put(scheduledTime, nextTimer);			
			return nextTimer;
		}
		
		cakeTimer = scheduledTimeTimerMap.get(scheduledTime);
		if (null != cakeTimer) {
			logger.debug("found existing timer via scheduledTime: " + scheduledTime);
			return cakeTimer;
		} 
		
		if (++currTimerIndex == timerTasks.size()) {
			currTimerIndex = 0;
		}
		
		logger.debug("Use next timer via index: " + currTimerIndex);
		nextTimer = timerTasks.get(currTimerIndex);
		cakeTimer = scheduledTimeTimerMap.put(scheduledTime, nextTimer);
		
		return nextTimer;		
	}		
}
