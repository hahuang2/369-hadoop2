package csc369;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortCountryAZ {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 3) return;

            String country = parts[0];
            StringBuilder url = new StringBuilder();
            for (int i = 1; i < parts.length - 1; i++) {
                if (i > 1) url.append(" ");
                url.append(parts[i]);
            }

            String countStr = parts[parts.length - 1];
            context.write(new Text(country), new Text(url.toString() + "\t" + countStr));
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text country, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String[]> list = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\\t");
                if (parts.length == 2) {
                    list.add(parts);
                }
            }

            list.sort((a, b) -> Integer.compare(
                Integer.parseInt(b[1]),
                Integer.parseInt(a[1])
            ));

            for (String[] entry : list) {
                context.write(country, new Text(entry[0] + "\t" + entry[1]));
            }
        }
    }
}
