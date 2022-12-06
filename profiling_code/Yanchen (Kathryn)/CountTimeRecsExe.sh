javac -classpath `yarn classpath` -d . CountTimeRecsMapper.java
javac -classpath `yarn classpath` -d . CountTimeRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountTimeRecs.java
jar -cvf CountTimeRecs.jar *.class
hadoop jar CountTimeRecs.jar CountTimeRecs project/clean/part-r-00000 /user/yz6956/project/profiling_time