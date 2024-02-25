package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CriticalPercentage implements Function<Tuple2<String, Double>, Boolean> {

	public double criticalThreshold;
	
	public CriticalPercentage(double crit)
	{
		this.criticalThreshold=crit;
	}

	public Boolean call(Tuple2<String, Double> criticalPercentage)  {

		if (criticalPercentage._2().doubleValue()>criticalThreshold)
			return true;
		else
			return false;
			
	}

}
