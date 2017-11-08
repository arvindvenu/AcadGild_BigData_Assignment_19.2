package com.acadgild;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HBaseBulkLoad {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// create the hbase configuration object 
		Configuration conf = HBaseConfiguration.create();
		
		// using the configuration object, create an HbaseAdmin object for DDL operations
		HBaseAdmin admin = new HBaseAdmin(conf);		
		
		// Create an HTableDescriptor object for the customer table
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("customer"));
		
		// Create an HColumnDescriptor object for the details column family
		HColumnDescriptor details = new HColumnDescriptor("details");
		
		// add the detail scolumn family to the Hbase table
		tableDescriptor.addFamily(details);

		System.out.println("Disabling and Dropping table if already exists...");
		
		// check is the customer table exists
		if (admin.isTableAvailable("customer")) {
			// if the customer table exists, is it enabled
			if (admin.isTableEnabled("customer")) {
				System.out.println("table exists and is enabled");
				
				// if the customer table exists and is enabled, disable it because Hbase requires us to disable the table before droppin it
				admin.disableTable("customer");
			}
			// drop the customer table
			admin.deleteTable("customer");
		}
		
		System.out.println("Creating HBase Table...");
		
		// create the customer table afresh
		admin.createTable(tableDescriptor);
		System.out.println("HBase Table Created!");
		
		// close the admin object
		admin.close();
		
		// input path to the file from which data needs to be loaded into the hbase table
		Path inputPath = new Path("/user/arvind/hbase/acadgild/assignments/assignment_18.2/input/customers.dat");
		
		// Initialize Htable object for the customer table
		HTable table=new HTable(conf,"customer");
		
		// set the output table property
		conf.set("hbase.mapred.outputtable", "customer");
		
		// initialize the Job instance
		Job job = Job.getInstance(conf,"HBase_Bulk_loader");
		
		// Set map output key type ImmutableBytesWritable
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		
		// Set map output value type to Put
		job.setMapOutputValueClass(Put.class);
		
		// disable speculative execution. So duplicate jobs for slow jobs are not created 
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		
		// input is a CSV file which is a variant of text file. so input format is TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);
		
		// out put will be to a hfile which in turn will be used to load the data into an Hbase table
		job.setOutputFormatClass(HFileOutputFormat.class);
		
		// set the driver class
		job.setJarByClass(HBaseBulkLoad.class);
		
		// set the mapper class
		job.setMapperClass(HBaseBulkLoad.ImportMapper.class);
		
		// set the input path from where CSV will be loaded 
		FileInputFormat.setInputPaths(job, inputPath);
		
		// set the output path where the hfile will be written to
		TextOutputFormat.setOutputPath(job, new Path("/user/arvind/hbase/acadgild/assignments/assignment_18.2/output"));
		
		// configure increematal load for the Hbase table 
		HFileOutputFormat.configureIncrementalLoad(job, table);
		
		// wait for job cmpletion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		
	}
	
	// this is the mapper class
	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			try {
				// read a single row of the input CSV and split it by comma
				String[] row = value.toString().split(",");
				
				// first element in the split array is the row key
				byte[] bRowKey = Bytes.toBytes(row[0]);
				
				// row key is the key which will be output by the mapper
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
				
				// do an insert operation
				Put p = new Put(bRowKey);
				
				// add column-value pairs to the column family for this row key 
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes(row[1]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes(row[2]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes(row[3]));
				
				// output the row key and the Put operation
				context.write(rowKey, p);
				
				// Output of every Mapper will be sorted on rowKey by the framework
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

