package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AggSelect implements Expression {

	private List<Aggregate> aggregations;
	private List<Integer> calPosi;
	
	public AggSelect() {
		aggregations = new ArrayList<Aggregate> ();
		calPosi = new ArrayList<Integer>();
	}
	
	public void addColumn(Aggregate aggregate) {
		aggregations.add(aggregate);
		
		//this is a calculation column
		if (aggregate.getColName().indexOf(Expression.TOKEN_SP) > 0) {
			calPosi.add(aggregations.size() - 1);
		}
	}
	
	public boolean contains(String fakeName) {
		for (int i=0;i<aggregations.size();++i) {
			if (aggregations.get(i).getFakeName().equals(fakeName))
				return true;
		}
		return false;
	}
	
	public Aggregate getColumn(int i) {
		return aggregations.get(i); 
	}
	
	public List<Integer> getCaculation() {
		return calPosi;
	}
	
	public List<Aggregate> getColumns() {
		return aggregations;
	}
	
	@Override 
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i=0;i<size;i++) {
			Aggregate agg = new Aggregate();
			agg.readFields(in);
			aggregations.add(agg);
		}
		
		//read the position of caculate columns
		size = in.readInt();
		for (int i=0;i<size;i++) {
			calPosi.add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(aggregations.size());
		for (int i=0;i<aggregations.size();++i) {
			aggregations.get(i).write(out);
		}
		
		//write the position of caculate columns
		out.writeInt(calPosi.size());
		for (int i=0;i<calPosi.size();++i) {
			out.writeInt(calPosi.get(i).intValue());
		}
	}
	
}
