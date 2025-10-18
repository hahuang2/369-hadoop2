package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sum {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+");
            if (parts.length < 2) return;

            String last = parts[parts.length - 1];
            int count;
            try {
                count = Integer.parseInt(last);
            } catch (NumberFormatException e) {
                return; // skip invalid lines
            }

            StringBuilder country = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                if (i > 0) country.append(" ");
                country.append(parts[i]);
            }

            context.write(new Text(country.toString()), new IntWritable(count));
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text country, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable count : counts) {
                total += count.get();
            }

            context.write(country, new IntWritable(total));
        }
    }
}