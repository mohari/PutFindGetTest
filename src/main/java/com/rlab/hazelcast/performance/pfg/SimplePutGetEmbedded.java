package com.rlab.hazelcast.performance.pfg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.rlab.hazelcast.utils.Utils;

/**
 * SimplePutGetEmbedded
 * This class runs a hazelcast server node and 
 * measures performance of put and get operations on
 * IMap
 * See constructor for parameters
 * @author Riaz
 *
 */
public class SimplePutGetEmbedded {

	HazelcastInstance hz ;
	private static final int KB = 1024;
	IMap<String, Object> map = null;
	HashMap<String,Future<Long>> stats;
	List<Long> taskDuration = new ArrayList<Long>();
	CountDownLatch cdl;
	int noOfThreads,findChunks;
	int count;
	int sizeOfKey_KB,sizeOfValue_KB;

	/**
	 * 
	 * @param noOfThreads = No of Parallel Thread
	 * @param sizeOfKey_KB  
	 * @param sizeOfValue_KB
	 * @param count  = no Objects for put / find / get
	 * @param findChunks = No of chunk for find operation [ count % fundChunk = 0 must be maintained]
	 */

	public SimplePutGetEmbedded(int noOfThreads, int sizeOfKey_KB, int sizeOfValue_KB,int count, int findChunks){
		try{
			hz=launchHazelcastInstance();
			stats = new HashMap<String,Future<Long>>();
			taskDuration = new ArrayList<Long>();

			map = hz.getMap("ObjectStore");

			this.findChunks = findChunks;
			this.count = count;
			this.noOfThreads=noOfThreads;
			this.sizeOfValue_KB=sizeOfValue_KB;
			this.sizeOfKey_KB=sizeOfKey_KB;

			cdl = new CountDownLatch(count);
			putTest();
			printStats(" PUT ");

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


			taskDuration.clear();
			cdl = new CountDownLatch(count);
			getTest();
			printStats(" GET "); 
		}catch(Exception e){
			e.printStackTrace();
		}

		// hz.shutdown();
	}

	private void putTest(){
		ExecutorService es = Executors.newFixedThreadPool(noOfThreads);
		for (int i = 0; i < count; i++) {
			String key=new String(createValue(sizeOfKey_KB));
			Future<Long> f = es.submit(new PutTask(key, createValue(sizeOfValue_KB)));
			stats.put(key, f);
		}

		try {
			cdl.await();
		} catch (InterruptedException e) {e.printStackTrace();}  

		es.shutdown();
	}



	private void getTest(){
		ExecutorService es = Executors.newFixedThreadPool(noOfThreads);
		for(Entry<String,Future<Long>> e : stats.entrySet()){
			String key=e.getKey();
			Future<Long> f = es.submit(new GetTask(key));
			stats.put(key, f);
		}
		try {
			cdl.await();
		} catch (InterruptedException e) {e.printStackTrace();}  

		es.shutdown();
	}

	private void printStats(String name) throws InterruptedException, ExecutionException{
		long min=0l;
		long max=0l;
		long sum=0l;
		long duration=0l;


		min=stats.entrySet().iterator().next().getValue().get();
		for(Entry<String,Future<Long>> e : stats.entrySet()){
			long lat=e.getValue().get();
			if(lat < min) min=lat;
			if(lat > max) max=lat;
			sum+=lat;
		}


		Collections.sort(this.taskDuration);
		duration=taskDuration.get(taskDuration.size()-1)-taskDuration.get(0);

		//System.out.println("SIZEEEEEEE :: "+taskDuration.size()+"  Duration "+sum);
		//System.out.println(findRunTimes);

		double min_ms=min/1000000.0;
		double max_ms=max/1000000.0;
		double tt_ms=duration/1000000.0;
		double avgT=((double)tt_ms)/count;

		System.out.println("OP, MIN(ms), MAX(ms), AvgT(ms), AvgT(s), Count, Total Time(ms), Key(KB),Value(KB)" );
		System.out.printf(name+", %f,%f,%f,%f,%d,%f,%d,%d \n"   ,min_ms
				, max_ms
				, avgT
				,(avgT/1000.0)
				, count
				, tt_ms
				,sizeOfKey_KB
				,sizeOfValue_KB);

	}

	private static byte[] createValue(int numberOfK) {
		return RandomUtils.nextBytes(numberOfK * KB);
	}

	private HazelcastInstance launchHazelcastInstance(){
		Config config;
		try {
			config = new FileSystemXmlConfig("src/main/resource/hazelcast-embedded.xml");
			return Hazelcast.newHazelcastInstance(config);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		new SimplePutGetEmbedded(20,1,1,1000,10);
	}

	class PutTask implements Callable<Long>{
		String key;
		Object value;
		PutTask(String key, Object value){
			this.key=key;
			this.value=value;
		}
		public Long call() {
			long ret=0;
			long t0=System.nanoTime();	 
			map.set(key,value);
			ret= System.nanoTime();
			taskDuration.add(t0);
			taskDuration.add(ret);
			ret=ret-t0;
			cdl.countDown();
			return ret;
		}
	}

	//use predicate or findAll
	class FindTask implements Callable<Long>{
		Set<String> keys;	
		FindTask(Set<String> keys){
			this.keys=keys;
		}
		public Long call() {
			long ret=0;
			long t0=System.nanoTime();	 
			Map<String,Object> res =  map.getAll(keys);
			ret= System.nanoTime();
			taskDuration.add(t0);
			taskDuration.add(ret);
			ret=ret-t0;
			cdl.countDown();
			return ret;
		}
	}

	class GetTask implements Callable<Long>{
		String key;
		GetTask(String key){
			this.key=key;
		}
		public Long call() {
			long ret=0;
			long t0=System.nanoTime();	 
			Object val = map.get(key);	
			ret= System.nanoTime();
			taskDuration.add(t0);
			taskDuration.add(ret);
			ret=ret-t0;
			cdl.countDown();
			return ret;
		}
	}


}
