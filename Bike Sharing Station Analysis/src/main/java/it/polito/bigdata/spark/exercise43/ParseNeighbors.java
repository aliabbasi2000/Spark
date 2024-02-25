package it.polito.bigdata.spark.exercise43;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ParseNeighbors implements PairFunction<String, String, List<String>> {

	@Override
	public Tuple2<String, List<String>> call(String line)  {
		
		String[] fields=line.split(",") ;
		
		String stationId=fields[0];
		String[] neighbors=fields[1].split(" ");
		
		List<String> listNeighbors=new ArrayList<String>(Arrays.asList(neighbors));
				
		return new Tuple2<String, List<String>>(stationId, listNeighbors);
	}

}
