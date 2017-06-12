package com.rlab.hazelcast.performance.timeseries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.rlab.hazelcast.utils.Utils;

public class PutFilterTimeSeries {

	HazelcastInstance hz;
	private static final int KB = 1024;

	Ringbuffer<TimeSeriesObject> rb;

	HashMap<String, Future<Long>> stats;
	List<Future<FilterResultInfo>> findStats;

	int noOfThreads;
	int count;
	int sizeOfKey_KB;

	/**
	 * 
	 * @param noOfThreads
	 *            = No of Parallel Thread
	 * @param sizeOfKey_KB
	 * @param sizeOfValue_KB
	 * @param count
	 *            = no Objects for put / find / get
	 * @param chunks
	 *            = No of chunk for find operation [ count % fundChunk = 0 must
	 *            be maintained]
	 */

	public PutFilterTimeSeries(int noOfThreads, int sizeOfKey_KB, int count) {
		try {
			hz = launchHazelcastInstance();
			stats = new HashMap<String, Future<Long>>();
			findStats = new ArrayList<Future<FilterResultInfo>>();

			// configure in xml file
			rb = hz.getRingbuffer("rb");
			System.out.println("RB Capacity " + rb.remainingCapacity());

			this.count = count;
			this.noOfThreads = noOfThreads;
			this.sizeOfKey_KB = sizeOfKey_KB;

			putTest();
			printStats(" PUT ");

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			filterTest();
			printStats(" FILTER ");

			// hz.shutdown();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void putTest() {
		ExecutorService es = Executors.newFixedThreadPool(1);

		Future<Long> f = es.submit(new PutTask());
		stats.put(" PUT ", f);

		es.shutdown();
	}

	private void filterTest() {
		ExecutorService es = Executors.newFixedThreadPool(1);
		Future<FilterResultInfo> f = es.submit(new FilterTask());
		// try {
		// cdl.await();
		// } catch (InterruptedException e) {e.printStackTrace();}
		findStats.add(f);

		es.shutdown();
	}

	private void printStats(String name) throws InterruptedException, ExecutionException {

		int res_size = 0;
		;
		int readCount = 0;
		long duration = 0l;

		if (name.equals(" FILTER ")) {
			FilterResultInfo fri = findStats.get(0).get();
			duration = fri.duration;
			res_size = fri.size;
			readCount = fri.readCount;

		} else {
			duration = stats.entrySet().iterator().next().getValue().get();
			res_size = count;
			readCount = count;
		}
		double tt_ms = duration / 1000000.0;
		double avgT = ((double) tt_ms) / readCount;

		System.out.println("OP, AvgT(ms), AvgT(s), Result Count, Total Time(ms), Total Read Count, Key(KB)");
		System.out.printf(name + ",%f,%f,%d,%f,%d,%d \n", avgT, (avgT / 1000.0), res_size, tt_ms, readCount,
				sizeOfKey_KB);
	}

	private static byte[] createValue(int numberOfK) {
		return RandomUtils.nextBytes(numberOfK * KB);
	}

	private HazelcastInstance launchHazelcastInstance() {
		ClientConfig config;
		try {
			XmlClientConfigBuilder xmlcfgb = new XmlClientConfigBuilder("src/main/resource/hazelcast-client.xml");
			return HazelcastClient.newHazelcastClient(xmlcfgb.build());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new PutFilterTimeSeries(20, 1, 10000);
	}

	class PutTask implements Callable<Long> {

		public Long call() throws InterruptedException, ExecutionException {
			long ret = 0;
			long t0, t1;

			ArrayList<TimeSeriesObject> ts_list = null;
			for (int k = 0; k < (count / 1000); k++) {
				ts_list = new ArrayList<TimeSeriesObject>();
				int val=k*1000;
				for (int i = 0; i < 1000; i++) {
					String key = new String(createValue(sizeOfKey_KB));
					ts_list.add(new TimeSeriesObject(key, System.nanoTime(), val+i));
				}
				t0 = System.nanoTime();
				long result = rb.addAllAsync(ts_list, OverflowPolicy.FAIL).get();
				ret = ret + (System.nanoTime() - t0);

			}

			System.out.println("PUT Result :  Buffer size " + rb.size() + "  head: " + rb.headSequence() + "tail: "
					+ rb.tailSequence());

			return ret;
		}
	}

	// use predicate or findAll
	class FilterTask implements Callable<FilterResultInfo> {

		public FilterResultInfo call() throws InterruptedException, ExecutionException {
			Thread.sleep(500);

			long ret = 0;

			int startSeq = new Random().nextInt(count / 2);
			int range = new Random().nextInt((int) (count - startSeq));
			if (range > 1000)
				range = 1000;

			System.out.println("Filter start ::  Buffer size " + rb.size() + "  head: " + rb.headSequence() + "tail: "
					+ rb.tailSequence());
			long t0 = System.nanoTime();
			ICompletableFuture<ReadResultSet<TimeSeriesObject>> f = rb.readManyAsync(startSeq, 0, range, null);
			ReadResultSet<TimeSeriesObject> rs = f.get();
			ret = System.nanoTime() - t0;

			int x = 0;
			for (TimeSeriesObject tso : rs) {
				System.out.println(tso);
				x++;
			}

			// cdl.countDown();
			FilterResultInfo fri = new FilterResultInfo(ret, x, rs.readCount());
			return fri;
		}

	}

	class FilterResultInfo {
		long duration;
		int readCount;
		int size;

		public FilterResultInfo(long duration, int size, int readCount) {
			super();
			this.duration = duration;
			this.size = size;
			this.readCount = readCount;
		}
	}

}
