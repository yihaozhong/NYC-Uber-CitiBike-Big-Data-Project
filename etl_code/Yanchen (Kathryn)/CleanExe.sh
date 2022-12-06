javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath`:. -d . Clean.java
jar -cvf Clean.jar *.class
hadoop jar Clean.jar Clean project/input/*.csv /user/yz6956/project/clean