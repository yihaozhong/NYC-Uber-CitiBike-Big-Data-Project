javac -classpath `yarn classpath` -d . CountMonthRecsMapper.java
javac -classpath `yarn classpath` -d . CountMonthRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountMonthRecs.java
jar -cvf CountMonthRecs.jar *.class
hadoop jar CountMonthRecs.jar CountMonthRecs project/clean/part-r-00000 /user/yz6956/project/profiling_month