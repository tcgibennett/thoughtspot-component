package com.thoughtspot.talend.components.output;

public class Table {

	private String[] fields = null;
	private String enclosing_character;
	
	public Table(int ttl, String enclosing_character) {
		fields = new String[ttl];
		if (enclosing_character == null)
			this.enclosing_character = "";
		else
			this.enclosing_character = enclosing_character;
	}
	
	public void add(String col, int idx)
	{
		fields[idx] = enclosing_character+col+enclosing_character;
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
