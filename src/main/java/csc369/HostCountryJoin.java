package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HostCountryJoin {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;


    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {

            String host = key.toString().trim();
            String country = value.toString().trim();

            if (!host.isEmpty() && !country.isEmpty()) {
                context.write(new Text(host), new Text("A\t" + country));
            }
        }
    }


    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;


            String[] tokens = line.split(" ");
            if (tokens.length > 0) {
                String host = tokens[0];
                context.write(new Text(host), new Text("B"));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> countries = new ArrayList<>();
            int requestCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\t", 2);
                if (parts[0].equals("A")) {
                    countries.add(parts[1]);
                } else if (parts[0].equals("B")) {
                    requestCount++;
                }
            }

            if (countries.isEmpty()) {
                countries.add("Unknown Location");
            }

            for (String c : countries) {
                context.write(new Text(c), new Text(String.valueOf(requestCount)));
            }
        }
    }
}
