javac -classpath `yarn classpath` -d . CountYearRecsMapper.java
javac -classpath `yarn classpath` -d . CountYearRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountYearRecs.java
jar -cvf CountYearRecs.jar *.class
hadoop jar CountYearRecs.jar CountYearRecs project/clean/part-r-00000 /user/yz6956/project/profiling_year