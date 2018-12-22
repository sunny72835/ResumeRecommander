import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class ResumeRecommender {

    static HashSet<String> skillSet = new HashSet<>();
    static String position = "None";

    public static class ResumeMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable score = new IntWritable();
            Text candidateIdText = new Text();
            String s = value.toString();


            int tempScore = 0;
            String candidateId = s.substring(0, s.indexOf(","));
            candidateIdText.set(candidateId);
            int resumeIndex = s.indexOf("\"");
            String resume;
            if(resumeIndex > -1)
                resume = s.substring(resumeIndex).toLowerCase();
            else
                resume = " ";

//            HashMap<String, Integer> map = new HashMap<>();//
            for(String skill: skillSet){
                int index = resume.indexOf(skill);
//                int skillScore = 0; //
                while(index < resume.length() && index != -1){
                    tempScore += 1;
//                    skillScore += 1; //
                    index = resume.indexOf(skill, index + 1);
                }
//                map.put(skill, skillScore); //
            }

//            for(Map.Entry<String, Integer> entry: map.entrySet()){//
//                context.write(new Text("=" + entry.getKey()), new IntWritable(entry.getValue()));//
//            }//

            score.set(tempScore);
            context.write(candidateIdText, score);
            if(!position.equals("none")){
                if(resume.indexOf(position) > -1)
                    context.write(new Text(candidateId + "-e"), new IntWritable(1));
            }

            for(String skill: skillSet){
                if(resume.contains(skill))
                    context.write(new Text("s="+skill), new IntWritable(1));
            }


        }
    }

    public static class ResumeReducer extends Reducer<Text, IntWritable, Text, Text> {
        HashMap<String, Integer> result1 = new HashMap<>();
        HashSet<String> result2 = new HashSet<>();
        HashMap<String, Integer> result3 = new HashMap<>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String id = key.toString();
            if(id.indexOf("-") == -1 && id.indexOf("=") == -1) {
                for (IntWritable score : values) {
                    if (score.get() > 0)
                        result1.put(key.toString(), score.get());       // There will be only one score per candidate id
                }
            }

            else if(id.indexOf("-") > -1){
                String candidateId = id.substring(0, id.indexOf("-"));
                result2.add(candidateId);
            }

            else if(id.indexOf("=") > -1){
                String skill = id.substring(id.indexOf("=") + 1);
                int value = 0;
                for(IntWritable i: values){
                    value += i.get();
                }
                result3.put(skill, value);
            }
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            context.write("=======Outcome 1: List of recommended candidates sorted based on skill score=======", new Text());
            int maxValue = -1;
            String key = "";
            String[] keys = new String[5];
            int[] vals = new int[5];
            int j = 0;
            for(int i = 0; i < 5; i++){
                for(Map.Entry<String, Integer> entry: result1.entrySet()){
                    if(entry.getValue() > maxValue){
                        key = entry.getKey();
                        maxValue = entry.getValue();
                    }
                }
                context.write(new Text(key+":\t"), new Text("" + result1.get(key)));
                keys[j] = key;
                vals[j] = result1.get(key);
                j++;
                result1.put(key, -1);
                maxValue = -1;
            }

            for(int i = 0; i < j; i++){
                result1.put(keys[i], vals[i]);
            }

            context.write("=======Outcome 2: List of experienced Candidates sorted based on their skill score=======", new Text());
            if(result2.size() < 1)
                context.write(new Text("No candidate found for given position"), new Text());
            else{
                HashMap<String, Integer> map = new HashMap<>();
                for(String s: result2) {

                    if (result1.containsKey(s))
                        map.put(s, result1.get(s));
                        //context.write(new Text(s), new Text("" + result1.get(s)));
                    else
                        map.put(s, 0);
//                    context.write(new Text(s), new Text("" + 0));
                }
                for (int i = 0; i < map.size(); i++){
                    int max = -1;
                    String k = "";
                    for(Map.Entry<String, Integer> entry: map.entrySet()){
                        if(entry.getValue() > max){
                            max = entry.getValue();
                            k = entry.getKey();
                        }
                    }
                    if(max == 0)
                        break;
                    context.write(new Text(k), new Text("" + max));
                    map.put(k, -1);
                }

            }

            context.write("=======Outcome 3:Sorted list of skills based on their popularity=======", new Text());
            int max = -1;
            String skill = "";
            for(int i = 0; i < result3.size(); i++){
                for(Map.Entry<String, Integer> entry: result3.entrySet()){
                    if(entry.getValue() > max){
                        skill = entry.getKey();
                        max = entry.getValue();
                    }
                }
                context.write(new Text(skill + ":\t"), new Text("" + result3.get(skill)));
                result3.put(skill, -1);
                max = -1;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long startTime = System.nanoTime();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <dataset_file> <outout_file> <input_file>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Scorecounter");
        job.setJarByClass(ResumeRecommender.class);
        job.setMapperClass(ResumeMapper.class);
        job.setReducerClass(ResumeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        File input = new File(args[2]);
        BufferedReader reader = new BufferedReader(new FileReader(input));
        String[] skills = reader.readLine().split(",");
        for(String s: skills)
            skillSet.add(s.toLowerCase());
        position = reader.readLine();
        if(position == null)
            position = "None";
        position = position.toLowerCase();
        boolean flag = job.waitForCompletion(true);
        long endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1000000;
        System.out.println("Duration: " + duration);
        System.exit(flag ? 0 : 1);
    }
}
