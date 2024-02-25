package it.polito.bigdata.spark.exercise43;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Count implements Serializable {
	public int numReadings;
	public int numCriticalReadings;
	
	public Count(int num, int numCritical)
	{
		this.numReadings=num;
		this.numCriticalReadings=numCritical;
	}
	
	public String toString()
	{
		return new String("total:"+this.numReadings+ " critical:"+this.numCriticalReadings);		
	}
	
}
