package com.acadgild;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Task3 {
	public static void main(String[] args) throws IOException, InterruptedException {
		
		// obtain an HBaseConfiguration for the localhost hbase instance
		Configuration conf = HBaseConfiguration.create();
		
		// create an Htable reference ro the customer table 
		System.out.println("Creating HTable instance to 'customer'...");
		HTable table = new HTable(conf, "customer");
		
		// create a new Scan object and restrict the search to the details column family
		System.out.println("Creating scan object...");
		Scan scan = new Scan();
		System.out.println("Narrowing down the result to details column family...");
		scan.addFamily(Bytes.toBytes("details"));
		
		// add a filter criteria to filter those rows whose location is AUS
		System.out.println("Adding single column value filter on scan object...");
		scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("details"), Bytes.toBytes("location"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("AUS"))));
		
		// Obtain a ResultScanner iterator from the HTable object 
		System.out.println("Getting a result scanner object...");
		ResultScanner rs = table.getScanner(scan);
		
		// iterate over the ResultScanner
		for (Result r : rs) {
			// obtain the row key
			String rowKey = new String(r.getRow());
			
			// obtain the columns from the 'details column family'
			String name = new String(r.getValue(Bytes.toBytes("details"), Bytes.toBytes("name")));
			String location = new String(r.getValue(Bytes.toBytes("details"), Bytes.toBytes("location")));
			int age = new Integer(new String(r.getValue(Bytes.toBytes("details"), Bytes.toBytes("age"))));
			
			// print out all the values
			System.out.println("rowKey:"+rowKey +"   name:"+name+"   location:"+location+"    age:"+age);
		}
		
		// close the scanned and the Htable as part of housekeeping
		System.out.println("Closing Scanner instance...");
		rs.close();
		table.close();
	}
}

