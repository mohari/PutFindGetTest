package com.rlab.hazelcast.performance.timeseries;

import java.io.Serializable;

public class TimeSeriesObject implements Serializable {
	
	private String key;
	private long time;
	private int value;
	
	
	
	
	public TimeSeriesObject(String key, long time, int value) {
		super();
		this.key = key;
		this.time = time;
		this.value = value;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		//result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + (int) (time ^ (time >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TimeSeriesObject other = (TimeSeriesObject) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TimeSeriesObject [key= [Not print / byte[] ] , time=" + time + ", value=" + value + "]";
	}
	
	

}
