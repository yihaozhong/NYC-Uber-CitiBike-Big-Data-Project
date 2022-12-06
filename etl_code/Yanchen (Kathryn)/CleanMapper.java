import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String[] line = value.toString().split(",");
        Text word = new Text();

        if (line.length == 13 && !line[0].contains("ride")) {
            String started_at = line[2];
            String ended_at = line[3];
            String start_lat = line[8];
            String start_lng = line[9];
            String end_lat = line[10];
            String end_lng = line[11];

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            LocalDateTime start = LocalDateTime.parse(started_at.substring(0, started_at.length()), formatter);
            LocalDateTime end = LocalDateTime.parse(ended_at.substring(0, started_at.length()), formatter);
            
            long sec_diff = java.time.Duration.between(start, end).getSeconds();

            if (sec_diff > 0) {
                word.set(started_at+","+ended_at+","+start_lat+","+start_lng+","+end_lat+","+end_lng);
                context.write(word, new Text(""));
            }
        }
        if (line.length == 15 && !line[0].contains("duration")) {
            String seconds = line[0];
            String starttime = line[1].substring(1, 20);
            String stoptime = line[2].substring(1, 20);
            String start_lat = line[5];
            String start_lng = line[6];
            String end_lat = line[9];
            String end_lng = line[10];

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            LocalDateTime start = LocalDateTime.parse(starttime, formatter);
            LocalDateTime end = LocalDateTime.parse(stoptime, formatter);
            
            long sec_diff = java.time.Duration.between(start, end).getSeconds();

            if (sec_diff > 0) {
                word.set(starttime+","+stoptime+","+start_lat+","+start_lng+","+end_lat+","+end_lng);
                context.write(word, new Text(""));
            }
        }
    }
}