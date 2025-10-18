package csc369;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HostCountryUrlJoin {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context)
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
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] tokens = line.split(" ");
            if (tokens.length > 6) { 
                String host = tokens[0];
                String url = tokens[6]; 
                context.write(new Text(host), new Text("B\t" + url));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> countries = new ArrayList<>();
            ArrayList<String> urls = new ArrayList<>();

            for (Text val : values) {
                String line = val.toString();
                if (line.startsWith("A\t")) {
                    countries.add(line.substring(2));
                } else if (line.startsWith("B\t")) {
                    urls.add(line.substring(2));
                }
            }

            if (countries.isEmpty()) {
                countries.add("Unknown");
            }

            for (String country : countries) {
                for (String url : urls) {
                    context.write(new Text(country), new Text(url));
                }
            }
        }
    }
}
