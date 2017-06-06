from subprocess import Popen, PIPE, STDOUT
import os
import argparse
import time 


if __name__ == '__main__':

	print(
"""Continuous Query 
>> declaring variables of sensor collection type      <DEFINE SensorCollection sensor_collection>
>> declaring variables of observation streams type    <DEFINE ObservationStream observation_stream>
>> generating sensor collection                       <sensor_collection = SELECT * FROM  sensor_collection sen, WHERE  sen.Type.name = "WiFi AP">
>> generating observation collection                  <observation_stream = SENSORS_TO_OBSERVATION_STREAM(sensor_collection)     
>> writing a final query on top of observation streams<SELECT count(*) FROM  observation_stream obs  RANGE  1 minutes SLIDE 1 minute>
>> duration                                           <duration = 1000>
======================================================

Query Inputs:
""")

	arguments = []
	process = Popen(['java', 'Parser'], stdout=PIPE, stderr=STDOUT)

	for line in process.stdout:
		arguments.append(line.rstrip())

	# continues query arguments
	[table, operation, colume, groupcolume, sql, window, slide, duration] = arguments

	print(arguments)
	topic = "test"
	name = "%s_" % (table) + str(int(time.time()))
	print ">>> query parse completed"
	print ">>> input arguments for spark-submit"
	# spark submit arguments
	spark_submit_args = "'%s' '%s' '%s' '%s' '%s' '%s' '%s' '%s' '%s' '%s'" %(table, operation, colume, groupcolume, sql, window, slide, duration, topic, name)
	spark_submit_path = "/Users/zhongzhuojian/Downloads/spark-2.1.0-bin-hadoop2.7/bin/spark-submit"
	spark_submit_jar = "/Users/zhongzhuojian/intellijworkspace/cs223new/out/artifacts/cs223new_jar/cs223new.jar"
	spark_submit = "%s --class cs223new.TIPPERSQueryEngineNew --master local[4] %s %s" %(spark_submit_path, spark_submit_jar, spark_submit_args)

	os.system(spark_submit)


"""
//[0] : tableName
//[1] : aggregate
//[2] : sql
//[3] : window size
//[4] : slide
//[5] : duration
//[6] : topic
//[7] : app name
"""