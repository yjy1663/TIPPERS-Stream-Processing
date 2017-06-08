import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

class QueryParser {
	private String range = "";
	private String slide = "";
	private String SensorCollection = "";
	private String ObservationStream = "";
	private String finalSQL = "";
	private String collectionQuery = "";
	private String aggFunc = "";
	private String table = "";
	private String duration = "";
	private String aggColumn = "";
	private String groupByColumn = "";
	private String whereColumn = "";


	public String getWhereColumn(){
		return whereColumn;
	}

	public void setWhereColumn(String whereColumn){
		this.whereColumn = whereColumn;
	}

	public String getGroupByColumn() {
		return groupByColumn;
	}

	public void setGroupByColumn(String groupByColumn) {
		this.groupByColumn = groupByColumn;
	}

	public String getAggFunc() {
		return aggFunc;
	}

	public void setAggFunc(String aggFunc) {
		this.aggFunc = aggFunc;
	}

	public String getAggColumn() {
		return aggColumn;
	}

	public void setAggColumn(String aggColumn) {
		this.aggColumn = aggColumn;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getSensorCollection() {
		return SensorCollection;
	}

	public void setSensorCollection(String sensorCollection) {
		SensorCollection = sensorCollection;
	}

	public String getObservationStream() {
		return ObservationStream;
	}

	public void setObservationStream(String observationStream) {
		ObservationStream = observationStream;
	}

	public String getFinalSQL() {
		return finalSQL;
	}

	public void setFinalSQL(String finalSQL) {
		this.finalSQL = finalSQL;
	}

	public String getCollectionQuery() {
		return collectionQuery;
	}

	public void setCollectionQuery(String collectionQuery) {
		this.collectionQuery = collectionQuery;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public void setSlide(String slide) {
		this.slide = slide;
	}

	// get how many seconds
	public String getRange() {
		return this.range;
	}
	
	// get how many seconds
	public String getSlide() {
		return this.slide;
	}
}


public class Parser {

	public void parse(String input, QueryParser queryParser) {
		//QueryParser queryParser = new QueryParser();
		
		String inputLowerCase = input.toLowerCase();
		if (inputLowerCase.startsWith("define")) {
			if (inputLowerCase.contains("sensorcollection")) {
				int index = inputLowerCase.indexOf("sensorcollection");
				//System.out.println(first.substring(index1 + 16));
				String sensorCollection = input.substring(index + 16).trim();
				queryParser.setSensorCollection(sensorCollection);
			}
			if (inputLowerCase.contains("observationstream")) {
				int index = inputLowerCase.indexOf("observationstream");
				String oS = input.substring(index + 17).trim();
				queryParser.setObservationStream(oS);
			}
		}
		if (inputLowerCase.startsWith("sensor_collection") && inputLowerCase.contains("=")) {
			int index = input.indexOf("=");
			String collectQ = input.substring(index + 1).trim();
			queryParser.setCollectionQuery(collectQ);
		}
		if (inputLowerCase.contains("sensors_to_observation_stream")) {
			int index = input.indexOf("=");
			String table = input.substring(0, index).trim();
			queryParser.setTable(table);
		}
//		SELECT count(*) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
//		SELECT min(dsd) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
//		SELECT max(fd) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
//		SELECT max(fd) FROM  observation_stream2 obs2  GROUP BY col1, col2 RANGE  1 minute SLIDE 1 minute DURATION 1000
//		SELECT * FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
//		SELECT * FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute 
//		SELECT * FROM  obs2  RANGE  1 minute SLIDE 1 minute
		if (inputLowerCase.contains("range") && inputLowerCase.contains("slide")) {
			int index1 = inputLowerCase.indexOf("range");
			int index2 = inputLowerCase.indexOf("slide");
			int index3 = inputLowerCase.indexOf("select");
			int index4 = inputLowerCase.indexOf("from");
			int index5 = inputLowerCase.indexOf("duration");
			int index7 = inputLowerCase.indexOf("group by");
			int index8 = inputLowerCase.indexOf("where");
			String range = input.substring(index1 + 5, index2).trim();
			String slide = "";
			String duration = "";
			String groupby = "";
			if (index5 == -1) {
				slide = input.substring(index2 + 5).trim();
			} else {
				slide = input.substring(index2 + 5, index5).trim();
				duration = input.substring(index5 + 8).trim();
			}
			if (index7 != -1) {
				groupby = input.substring(index7 + 8, index1).trim();
				String groupWithoutComma = "";
				if (groupby.contains(",")) {
					String[] groupArr = groupby.split(",");
					for(int i = 0; i < groupArr.length; i++) {
						groupArr[i] = groupArr[i].trim();
					}
					int size = groupArr.length;
					for (int i = 0; i < size - 1; i++) {
						groupWithoutComma += groupArr[i] + ",";
						//System.out.println(groupWithoutComma);
					}
					//System.out.println("?" + groupArr[size - 1]);
					groupWithoutComma += groupArr[size - 1];
					//System.out.println(groupWithoutComma);
					groupby = groupWithoutComma;
				}
				queryParser.setGroupByColumn(groupby);
			}
			String finalSQL = "";
			if (index7 != -1) {
				finalSQL = input.substring(0, index7).trim();
			} else {
				finalSQL = input.substring(0, index1).trim();
			}
			
			String operator = input.substring(index3 + 6, index4).trim();
			if (operator.toLowerCase().contains("sum") || operator.toLowerCase().contains("min") || operator.toLowerCase().contains("max") || 
					operator.toLowerCase().contains("mean")) {
				int index6 = operator.indexOf("(");
				String aggColumn = operator.substring(index6 + 1, operator.length() - 1);
				String aggFunc = operator.substring(0, index6);
				queryParser.setAggColumn(aggColumn);
				queryParser.setAggFunc(aggFunc);
			} else if (operator.toLowerCase().contains("count")) {
				queryParser.setAggFunc(operator.toLowerCase());
			} 
			String[] tableArr = null;
			if (index7 != -1) {
				if (index8 != -1) {
					tableArr = input.substring(index4 + 4, index8).trim().split(" ");
					queryParser.setWhereColumn(input.substring(index8 + 5, index7).trim());
				} else {
					tableArr = input.substring(index4 + 4, index7).trim().split(" ");
				}
			} else if (index1 != -1) {
				if (index8 != -1) {
					tableArr = input.substring(index4 + 4, index8).trim().split(" ");
					queryParser.setWhereColumn(input.substring(index8 + 5, index1).trim());
				} else {
					tableArr = input.substring(index4 + 4, index1).trim().split(" ");
				}
			}
			String tableName = "";
			if (tableArr.length > 0) {
				tableName = tableArr[0].trim();
			} else {
				tableName = "table";
			}
			queryParser.setRange(range);
			queryParser.setSlide(slide);
			queryParser.setFinalSQL(finalSQL);
			queryParser.setTable(tableName);
			queryParser.setDuration(duration);
		}
//		if (inputLowerCase.contains("duration")) {
//			int index = inputLowerCase.indexOf("=");
//			String duration = input.substring(index + 1).trim();
//			queryParser.setDuration(duration);
//		}
		//return queryParser;
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String input = "SELECT count(*) FROM  observation_stream2 obs2  RANGE  '1 minutes' SLIDE '1 minute'";
		Parser parser = new Parser();
		Scanner sc = new Scanner(System.in);
		QueryParser queryParser = new QueryParser();
		/*  TestCase
		 * DEFINE SensorCollection  sensor_collection2
			DEFINE ObservationStream  observation_stream2
		sensor_collection2 = SELECT * FROM  sensor_collection1 sen, WHERE  sen.Type.name = "WiFi AP"
		observation_stream2 = SENSORS_TO_OBSERVATION_STREAM( sensor_collection2 )
		~~~~~~
		SELECT count(*) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT min(dsd) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT max(fd) FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT max(fd) FROM  observation_stream2 obs2  GROUP BY col1, col2 RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT * FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT * FROM  observation_stream2 obs2  GROUP BY col1 RANGE  1 minute SLIDE 1 minute 
		SELECT * FROM  observation_stream2 obs2  RANGE  1 minute SLIDE 1 minute 
		SELECT min(dsd) FROM  observation_stream2 obs2  GROUP BY col1 RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT * FROM  obs2  GrOUP by col1, col2 RANGE  1 minute SLIDE 1 minute 
		SELECT count(*) FROM  observation_stream2 obs2  GROUP BY col RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT count(*) FROM  obs2 where XXX GROUP BY col RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT count(*) FROM  obs2 dsd where XXX GROUP BY col RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT count(*) FROM  obs2 GROUP BY col RANGE  1 minute SLIDE 1 minute DURATION 1000
		SELECT count(*) FROM  obs2 RANGE  1 minute SLIDE 1 minute 
		SELECT * FROM  obs2   sdfs  GROUP BY col1 RANGE  1 minute SLIDE 1 minute 
		~~~~~~~
		 */
		
		
		/*
		 * val tableName = args(0)
    val aggregate = args(1)
    val aggColumn = args(2)
    val groupByColumn = args(3)
    val sql = args(4)
    val windowSize = args(5)
    val slide = args(6)
    val duration = args(7)
    val topics = args(8)
    val appName = args(9)
		 */
		
		int counter = 1;
		while (sc.hasNextLine()) {
			String string = sc.nextLine();
			parser.parse(string, queryParser);
			if (counter == 1) {
				sc.close();
				break;
			}
			counter++;
		}
		System.out.println(queryParser.getTable());
		System.out.println(queryParser.getAggFunc());
		System.out.println(queryParser.getAggColumn());

		System.out.println(queryParser.getGroupByColumn());
		System.out.println(queryParser.getFinalSQL());
		System.out.println(queryParser.getRange());
		System.out.println(queryParser.getSlide());
		System.out.println(queryParser.getDuration());
		System.out.println(queryParser.getWhereColumn());
	}
}
