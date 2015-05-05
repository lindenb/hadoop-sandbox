// http://jayunit100.blogspot.fr/2013/07/hadoop-processing-headers-in-mappers.html
	
package com.github.lindenb.hadoop;
import htsjdk.samtools.util.AbstractIterator;
import htsjdk.tribble.readers.LineIterator;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class Test {
	  private static final Logger LOG = Logger.getLogger(Test.class);
	  
	  
	  public static class VcfRow
	  	implements WritableComparable<VcfRow>
	  	 {
		 private List<String> headerLines;
		 private String line;
		 private VariantContext ctx=null;
		 private VCFHeader header =null;
		 private VCFCodec codec=new VCFCodec();
		 public VcfRow()
		  	{
			this.headerLines = Collections.emptyList();
			this.line="";
		  	}
		 public VcfRow(List<String> headerLines,String line)
		 	{
			this.headerLines=headerLines; 
			this.line=line;
		 	}
		 
		@Override
		public void write(DataOutput out) throws IOException
			{
			out.writeInt(this.headerLines.size());
			for(int i=0;i< this.headerLines.size();++i)
				{
				out.writeUTF(this.headerLines.get(i));
				}
			byte array[]=line.getBytes();
			out.writeInt(array.length);
			out.write(array);
			}

		@Override
		public void readFields(DataInput in) throws IOException
			{
			int n= in.readInt();
			this.headerLines=new ArrayList<String>(n);
			for(int i=0;i<n;++i) this.headerLines.add(in.readUTF());
			n = in.readInt();
			byte array[]=new byte[n];
			in.readFully(array);
			this.line=new String(array);
			this.codec=new VCFCodec();
			this.ctx=null;
			this.header=null;
			}
		
		public VCFHeader getHeader()
			{
			if(this.header==null)
				{
				this.header = (VCFHeader)this.codec.readActualHeader(new MyLineIterator());
				}
			return this.header;
			}
		
		public VariantContext getVariantContext()
			{
			if(this.ctx==null)
				{
				if(this.header==null) getHeader();//force decode header
				this.ctx=this.codec.decode(this.line);
				}
			return this.ctx;
			}
		
		@Override
		public int compareTo(VcfRow o)
			{
			int i = this.getVariantContext().getContig().compareTo(o.getVariantContext().getContig());
			if(i!=0) return i;
			i = this.getVariantContext().getStart() - o.getVariantContext().getStart();
			if(i!=0) return i;
			i =  this.getVariantContext().getReference().compareTo( o.getVariantContext().getReference());
			if(i!=0) return i;
			return this.line.compareTo(o.line);
			}
		
		private  class MyLineIterator
			extends AbstractIterator<String>
			implements LineIterator
			{	
			int index=0;
			@Override
			protected String advance()
				{
				if(index>= headerLines.size()) return null;
				return headerLines.get(index++);
				}
			}
	  	}
	  
	  
	  public static class VcfInputFormat extends FileInputFormat<LongWritable, VcfRow>
	  	{
		
		private List<String> headerLines=new ArrayList<String>();
		
		@Override
		public RecordReader<LongWritable, VcfRow> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new VcfRecordReader();
			}  
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
			}
		 
		//LineRecordReader
		 private class VcfRecordReader extends RecordReader<LongWritable, VcfRow>
		  	{
			private LineRecordReader delegate=new LineRecordReader();
			public VcfRecordReader() throws IOException
			 	{
			 	}
			
			 @Override
			public void initialize(InputSplit genericSplit,
					TaskAttemptContext context) throws IOException {
				 delegate.initialize(genericSplit, context);
				while( delegate.nextKeyValue())
					{
					String row = delegate.getCurrentValue().toString();
					if(!row.startsWith("#")) throw new IOException("Bad VCF header");
					headerLines.add(row);
					if(row.startsWith("#CHROM")) break;
					}
			 	}
			 @Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return delegate.getCurrentKey();
			 	}
			 
			 @Override
			public VcfRow getCurrentValue() throws IOException,
					InterruptedException {
			Text row = this.delegate.getCurrentValue();
			return new VcfRow(headerLines,row.toString());
			 }
			 
			 @Override
			public float getProgress() throws IOException, InterruptedException {
				return this.delegate.getProgress();
			 	}
			 
			 @Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				return this.delegate.nextKeyValue();
			 	}
			 
			 @Override
			public void close() throws IOException {
				 delegate.close();
			 }
		  	}
	  	}
	  
	  /** mapper */
	  public static class VariantMapper
	       extends Mapper<LongWritable, VcfRow, Text, IntWritable>{

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, VcfRow vcfRow, Context context ) throws IOException, InterruptedException {
	    	VariantContext ctx = vcfRow.getVariantContext();
	    	if( ctx.isIndel())
	    		{	
	    		word.set("ctx_indel");
	 	        context.write(word, one);
	    		}
	    	if( ctx.isBiallelic())
	    		{	
	    		word.set("ctx_biallelic");
	 	        context.write(word, one);
	    		}
	    	if( ctx.isSNP())
	    		{	
	    		word.set("ctx_snp");
	    		context.write(word, one);
	    		}	
	    	if( ctx.hasID())
	    		{	
	    		word.set("ctx_id");
	    		context.write(word, one);
	    		}	
	    	word.set("ctx_total");
    		context.write(word, one);
	    	
    		for(String sample: vcfRow.getHeader().getSampleNamesInOrder())
    			{
    			Genotype g =vcfRow.getVariantContext().getGenotype(sample);
    			word.set(sample+" "+ctx.getType()+" "+g.getType().name());
        		context.write(word, one);
    			}
	    
	    	}
	  }

	  public static class IntSumReducer
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }

	  static public class VcfFileFilter implements PathFilter
	  	{
		@Override
		public boolean accept(Path path)
			{
			LOG.info(path);
			return path.getName().endsWith(".vcf");
			}  
	  	}
	  
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	   // FileSystem fs=FileSystem.get(conf);
	    
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Test.class);
	    job.setMapperClass(VariantMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	   
	    
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    Path inputPath=new Path(args[0]);
	    job.setInputFormatClass(VcfInputFormat.class);
	    
	    FileInputFormat.addInputPath(job, inputPath);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
