import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CountYearRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String[] line = value.toString().split(",");
        if(line.length > 1) {
            String starttime = line[0];
            String year = starttime.substring(0,4);
            context.write(new Text(year), new IntWritable(1));
        }
    }
}