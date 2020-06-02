package com.thoughtspot.talend.components.output;

public class Table {

	private String[] fields = null;
	
	public Table(int ttl) {
		fields = new String[ttl];
	}
	
	public void add(String col, int idx)
	{
		fields[idx] = col;
	}

	@Override
	public String toString() {
		StringBuilder buff = new StringBuilder();
		int idx = 0;
		for (String field : fields)
		{
			buff.append(field);
			if (idx++ != fields.length - 1)
				buff.append(",");
		}

		return buff.toString();
	}
	
	
}
