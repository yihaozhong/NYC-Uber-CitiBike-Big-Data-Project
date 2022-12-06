javac -classpath `yarn classpath` -d . CountEachMonthRecsMapper.java
javac -classpath `yarn classpath` -d . CountEachMonthRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountEachMonthRecs.java
jar -cvf CountEachMonthRecs.jar *.class
hadoop jar CountEachMonthRecs.jar CountEachMonthRecs project/clean/part-r-00000 /user/yz6956/project/profiling_eachmonth