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
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IMap;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.rlab.hazelcast.utils.Utils;

public class FilterTimeSeries {

	HazelcastInstance hz;
	private static final int KB = 1024;

	Ringbuffer<TimeSeriesObject> rb;

	List<Future<FilterResultInfo>> findStats;
	CountDownLatch cdl;

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

	public FilterTimeSeries(int noOfThreads, int sizeOfKey_KB, int count) {
		try {
			hz = launchHazelcastInstance();
			findStats = new ArrayList<Future<FilterResultInfo>>();

			// configure in xml file
			rb = hz.getRingbuffer("rb");

			this.count = count;
			this.noOfThreads = noOfThreads;
			this.sizeOfKey_KB = sizeOfKey_KB;
			//filterTest();
			
			// hz.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void filterTest(int startSeq, int range,IFunction<TimeSeriesObject,Boolean> filterFunction) throws InterruptedException, ExecutionException {
		findStats.clear();
		int noOf_K_Batches = range/1000;
		int lastBatchSize = range % 1000;
		cdl =new CountDownLatch(noOf_K_Batches+((lastBatchSize>0)?1:0));
		ExecutorService es = Executors.newFixedThreadPool(this.noOfThreads);
		for (int i=0;i<= noOf_K_Batches;i++){
			Future<FilterResultInfo> f=null;
			if(i==noOf_K_Batches && lastBatchSize > 0){
				 f = es.submit(new FilterTask(startSeq+(i*1000),lastBatchSize,filterFunction));
				 
			}else if (i < noOf_K_Batches){
			     f = es.submit(new FilterTask(startSeq+(i*1000),1000,filterFunction));
			}
			if(f != null)
			findStats.add(f);
		}
		
		cdl.await();
				
		es.shutdown();
		printStats(false,range);
	}

	private void printStats(boolean print,int range) throws InterruptedException, ExecutionException {

		int readCount = 0;
		long duration = 0l;

		for(Future<FilterResultInfo>f :this.findStats){
			FilterResultInfo fri =f.get();
			readCount +=fri.readCount;
			duration+=fri.duration;
			if(print){
				printResults(fri.rs,10);
			}
		}
		
		double tt_ms = duration / 1000000.0;
		double avgT = ((double) tt_ms) / range;

		System.out.println("OP, AvgT(ms), AvgT(s),  Total Time(ms), Range,Total Read Count, Key(KB)");
		System.out.printf("FILTER" + ",%f,%f,%f,%d,%d,%d \n", avgT, (avgT / 1000.0), tt_ms, range,readCount,
				sizeOfKey_KB);
	}
	
	public void printResults(ReadResultSet<TimeSeriesObject> rs, int limit){
		int i=0;
		System.out.println("==>>");
		for(TimeSeriesObject tso: rs){
			System.out.println(tso);
			if(++i >= limit) break;
		}
		System.out.println("<<==");
	}

	
	private HazelcastInstance launchHazelcastInstance() {
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
		FilterTimeSeries fts =new FilterTimeSeries(20, 1, 10000);
		try {
			fts.filterTest(0, 5000, null);
			// get all even 
			fts.filterTest(0, 5000, (ts) -> ts.getValue() % 2 == 0 ? true : false);
			// get all where value % 10 = 0
			fts.filterTest(0, 5000, (ts) -> ts.getValue() % 10 == 0 ? true : false);
			
			// get all odds
			fts.filterTest(0, 5000, (ts) -> ts.getValue() % 2 == 0 ? false : true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// use predicate or findAll
	class FilterTask implements Callable<FilterResultInfo> {
		
		long startSeq;
		int range;
		IFunction<TimeSeriesObject,Boolean> filterFunction;
		
		FilterTask (long startSeq,int range, IFunction<TimeSeriesObject,Boolean> filterFunction){
			this.startSeq=startSeq;
			this.range=range;
			this.filterFunction=filterFunction;
		}

		public FilterResultInfo call() throws InterruptedException, ExecutionException {
			Thread.sleep(500);
			try{
			long ret = 0;
			System.out.println("Filter start ::  Buffer size " + rb.size() + "  head: " + rb.headSequence() + "tail: "
					+ rb.tailSequence()+" filterStartSeq "+this.startSeq+" filterRange "+this.range);
			long t0 = System.nanoTime();
			ICompletableFuture<ReadResultSet<TimeSeriesObject>> f = rb.readManyAsync(startSeq, 0, range,
					filterFunction);
			ret = System.nanoTime() - t0;
			ReadResultSet<TimeSeriesObject> rs = f.get();
			 
			FilterResultInfo fri = new FilterResultInfo(ret, rs.readCount());
			fri.rs=rs;
			return fri;
			}finally{
				cdl.countDown();
			}
		}

	}

	class FilterResultInfo {
		long duration;
		int readCount;
		ReadResultSet<TimeSeriesObject> rs;

		public FilterResultInfo(long duration, int readCount) {
			super();
			this.duration = duration;
			this.readCount = readCount;
		}

		public ReadResultSet<TimeSeriesObject> getRs() {
			return rs;
		}

		public void setRs(ReadResultSet<TimeSeriesObject> rs) {
			this.rs = rs;
		}
		
		
	}

}
