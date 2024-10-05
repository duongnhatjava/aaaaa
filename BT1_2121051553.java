import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BT1_2121051553 {
    
    // Mapper class
    public static class ElectricityMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text year = new Text();
        private IntWritable avgConsumption = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into an array of strings
            String[] fields = value.toString().split("\\s+");

            // Extract year and calculate average consumption
            String yearString = fields[0];
            year.set(yearString);

            int sum = 0;
            int months = 12; // there are 12 months in each row

            for (int i = 1; i <= months; i++) {
                sum += Integer.parseInt(fields[i]);
            }
            int avg = sum / months;
            avgConsumption.set(avg);

            // Write year and average consumption as key-value
            context.write(year, avgConsumption);
        }
    }

    // Reducer class
    public static class ElectricityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                if (value.get() > 30) { // Filter years with average consumption > 30
                    context.write(key, value);
                }
            }
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Electricity Consumption Filter");

        job.setJarByClass(BT1_2121051553.class);
        job.setMapperClass(ElectricityMapper.class);
        job.setReducerClass(ElectricityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}S