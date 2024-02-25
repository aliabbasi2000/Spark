package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class Sum implements Function2<Count, Count, Count> {

	public Count call(Count value1, Count value2) {
		return new 
				Count(value1.numReadings+value2.numReadings, 
					value1.numCriticalReadings+value2.numCriticalReadings);
	}

}
