package test;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Calendar;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;


public class TestGetSetBenchmark {
  private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);
  private static final int TOTAL_OPERATIONS = 100000;

  public static void main(String[] args) { //  throws UnknownHostException, IOException {
	  testGetSetBenchmark (true, TOTAL_OPERATIONS);
  }
  
  public static int testGetSetBenchmark (boolean flushCacheFlag, int maxNumOps) {
	  int numOps = 0;
	  StringBuffer key;
	  try {
		    Jedis jedis = new Jedis(hnp.getHost(), hnp.getPort());
		    
		    System.out.println ("hnp.getHost():" + hnp.getHost() + " - maxNumOps:" + maxNumOps);
		    jedis.connect();
		    if (flushCacheFlag) {
			    jedis.flushAll();		    	
		    }

		    Object value;
		    key = new  StringBuffer ("foo");
		    long begin = Calendar.getInstance().getTimeInMillis();

		    for (numOps = 0; numOps <= maxNumOps; numOps++) {
		      jedis.set(key.toString() + numOps, "bar" + numOps);
		    }
		    long elapsed = Calendar.getInstance().getTimeInMillis() - begin;
		    System.out.println("Set maxNumOps: " + maxNumOps + " elapsed(ms): " + elapsed + " - in sec:" + (float)((float)elapsed / 1000.0));
		    float opsrate = (float)maxNumOps / (float)elapsed;
		    System.out.println("Set ops/ms = " + opsrate + " - ops/sec: " + opsrate * 1000);

		    begin = Calendar.getInstance().getTimeInMillis();
		    for (numOps = 0; numOps <= maxNumOps; numOps++) {
		      value = jedis.get(key.toString() + numOps);
		    }
		    elapsed = Calendar.getInstance().getTimeInMillis() - begin;
		    System.out.println("Get maxNumOps: " + maxNumOps + " elapsed(ms): " + elapsed + " - in sec:" + (float)((float)elapsed / 1000.0));
		    opsrate = (float)maxNumOps / (float)elapsed;
		    System.out.println("Get ops/ms = " + opsrate + " - ops/sec: " + opsrate * 1000);

		    jedis.disconnect();

		    // System.out.println(((1000 * 2 * maxNumOps) / elapsed) + " ops");
		  
	  } catch (Throwable exc) {
		  exc.printStackTrace();
	  }
	  
	  return numOps;
  }
}