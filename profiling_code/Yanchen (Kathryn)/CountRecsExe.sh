javac -classpath `yarn classpath` -d . CountRecsMapper.java
javac -classpath `yarn classpath` -d . CountRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountRecs.java
jar -cvf CountRecs.jar *.class
hadoop jar CountRecs.jar CountRecs project/clean/part-r-00000 /user/yz6956/project/profiling_count