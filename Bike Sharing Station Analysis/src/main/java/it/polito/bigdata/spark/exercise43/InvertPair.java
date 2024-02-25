package it.polito.bigdata.spark.exercise43;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class InvertPair implements PairFunction<Tuple2<String, Double>, Double, String> {

	@Override
	public Tuple2<Double, String> call(Tuple2<String, Double> pair) throws Exception {
		return new Tuple2<Double, String>(pair._2(), pair._1());
	}

}
