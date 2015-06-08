package com.getcake.geo.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;

import com.getcake.geo.model.GeoInfo;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import test.HostAndPortUtil;

public class ElasticCacheDao extends BaseDao {

	private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);


	private String sqlGetIpLookup;


	private String sqlGetLocationInfo;

	public void setSqlGetIpLookup (String sqlGetIpLookup) {
		this.sqlGetIpLookup = sqlGetIpLookup;
	}

	public void setSqlGetLocationInfo (String sqlGetLocationInfo) {
		this.sqlGetLocationInfo = sqlGetLocationInfo;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCache (boolean flushCacheFlag, int lowerImportId, int upperImportId){

		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		int bsStartNum;
		short bytestartNum, byteEndNum;
		Jedis jedis = null;

		try {

			// logger.debug("findLocationId: bytes conversion ipv6 bytes: " + bytesutf8);
			jedis = new Jedis(hnp.getHost(), hnp.getPort());
			System.out.println ("hnp.getHost():" + hnp.getHost());
			jedis.connect();
			if (flushCacheFlag) {
				jedis.flushAll();
			}

			startTime = Calendar.getInstance().getTimeInMillis();

			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT TOP 3 SUBSTRING(t.ipv6_start, 1, 3) as bytePrefixStr, SUBSTRING(t.ipv6_start, 4, 4) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 4, 4) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 4) != SUBSTRING(t.ipv6_end, 1, 4)  " +
						"and SUBSTRING(t.ipv6_start, 1, 3) = SUBSTRING(t.ipv6_end, 1, 3)  " +
						"order by bytePrefixStr, byteStartStr;" );

			/*sourceStmt.setInt(1, lowerImportId);
			sourceStmt.setInt(2, upperImportId); */
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");

				bsStartNum = Integer.parseInt(bytePrefixStr, 16);
				bytestartNum = Short.parseShort(byteStartStr, 16);
				byteEndNum = Short.parseShort(byteEndStr, 16);
				for (short range = bytestartNum; range < byteEndNum; range++) {
					jedis.set(Integer.toString(bsStartNum), Short.toString(range));
				}

				if ((count % 1000) == 0) {
					logger.debug ("transferIpv6Lookup- count:" + count + " - import_id:" + sourceRs.getInt("import_id") +
							" location_id:" + sourceRs.getInt("location_id") +
							" - ipv6_start: " + sourceRs.getBytes("ipv6_start") +
							" - ipv6_end: " + sourceRs.getBytes("ipv6_end"));
				}
			}
			endTime = Calendar.getInstance().getTimeInMillis();

			logger.debug("MsSqlDao - transferIpv6Lookup insert duration(ms):" + (endTime - startTime));

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
			if (jedis != null) {
				jedis.close();
				jedis.disconnect();
			}
		}
		return count;
    }

	public int loadDummyData (boolean flushCacheFlag, int maxNumOps) {

		int numOps = 0;
		byte b1 = 15, b2 = 127, b3 = -127;
		StringBuffer key;

		try {

		Jedis jedis = new Jedis(hnp.getHost(), hnp.getPort());

		System.out.println ("hnp.getHost():" + hnp.getHost() + " - maxNumOps:" + maxNumOps);
		jedis.connect();
		// jedis.auth(""); // foobared
		jedis.flushAll();

		Object value;
		key = new  StringBuffer ("foo");
		long begin = Calendar.getInstance().getTimeInMillis();

		for (numOps = 0; numOps <= maxNumOps; numOps++) {
			jedis.set(key.toString() + numOps, "bar" + numOps);
			// jedis.set(key.toString() + numOps, Short.valueOf((short)(numOps % 256)));
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
