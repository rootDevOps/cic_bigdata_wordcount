package practicas;

/**
 * Libreria de extends Mapper
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Libreria de extends Reduce
 */
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Libreria de main
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Librerias Generales
 */
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Empieza la clase para contar palabras: WordCount
 */
public class WordCount {

    /**
     * Contar el numero de palabras diferentes
     */
    public static class Map extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable uno = new IntWritable(1);
        private Text word = new Text();

        /**
         * Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
         *     Asigna un solo k,v
         * @param key :  the input key
         * @param value : the input value
         * @param context : collects mapped keys and values
         * @throws IOException : Señal que se ha producido una excepción de E / S de algún tipo.
         * @throws InterruptedException :
         * Referrer: https://hadoop.apache.org/docs/r2.7.4/api/org/apache/hadoop/mapreduce/Mapper.html
         */

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer search = new StringTokenizer(value.toString());
            while (search.hasMoreTokens()) {
                word.set(search.nextToken());
                context.write(word, uno);
            }
        }

    }
    public static class Red extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        /**
         * Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
         *     Este método se llama una vez para cada clave.
         * @param key : the input key
         * @param values : the input value
         * @param context : collects mapped keys and values
         * @throws IOException : Señal que se ha producido una excepción de E / S de algún tipo.
         * @throws InterruptedException :
         * Referrer: https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapreduce/Reducer.html
         */

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    /**
     * Identificar el top 10 de las palabras más concurrentes
     */



    /**
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length == 0){
            System.err.println("Usage: ... WordCount_1 <input source> <output dir>");
            System.exit(1);
        }else {

            Configuration conf = new Configuration();

            /**
             * Cuenta las palabras de un archivo
             *  	getInstance() - Creates a new Job with no particular Cluster.
             *   	setJarByClass(Class<?> cls) - Set the Jar by finding where a given class came from.
             *      setMapperClass(Class<? extends Mapper> cls) - Set the Mapper for the job.
             *      setCombinerClass(Class<? extends Reducer> cls) - Set the combiner class for the job.
             *      setReducerClass(Class<? extends Reducer> cls) - Set the Reducer for the job.
             *      setOutputKeyClass(Class<?> theClass) - Set the key class for the job output data.
             *      setOutputValueClass(Class<?> theClass) - Set the value class for job outputs.
             *  Referrer: https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/mapreduce/Job.html
             *      FileInputFormat.addInputPath(JobConf conf, Path path) -  Add a Path to the list of inputs for the map-reduce job.
             *  Referrer: https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/mapred/FileInputFormat.html
             *      setOutputPath(JobConf conf, Path outputDir) - Set the Path of the output directory for the map-reduce job.
             *  Referrer: https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/mapred/FileOutputFormat.html
             */

            Job job = Job.getInstance(conf, "Contador de Palabras: ");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(Map.class);  /* extends Mapper */
            job.setCombinerClass(Red.class); /* extends Reducer */
            job.setReducerClass(Red.class); /* extends Reducer */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


}
