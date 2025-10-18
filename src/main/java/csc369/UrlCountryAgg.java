package csc369;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlCountryAgg {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\\s+", 2);
            if (parts.length == 2) {
                String url = parts[0];
                String country = parts[1];
                context.write(new Text(url), new Text(country));
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text url, Iterable<Text> countries, Context context)
                throws IOException, InterruptedException {

            Set<String> unique = new TreeSet<>(); 
            for (Text c : countries) {
                unique.add(c.toString());
            }

            String joined = String.join(", ", unique);
            context.write(url, new Text(joined));
        }
    }
}
