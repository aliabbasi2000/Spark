package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class Percentage implements Function<Count, Double> {

	public Double call(Count value)  {
		double perc;
		
		perc=(double)value.numCriticalReadings/(double)value.numReadings;
		
		return new Double(perc);
	}

}
