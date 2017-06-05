package com.rlab.hazelcast.performance.pfg;

import java.io.File;
import java.io.FileNotFoundException;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
public class ServerMember {
    public static void main(String[] args) {
    	Config config;
		try {
			config = new FileSystemXmlConfig("src/main/resource/hazelcast.xml");
			HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}

//vm options -Xms4G -Xmx4G -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseG1GC -Dhazelcast.partition.count=271