package master;

import java.io.Serializable;

public class RunningData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3101241722714955250L;
	
	private long cid;
	private String name;
	public long getCid() {
		return cid;
	}
	public void setCid(long cid) {
		this.cid = cid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

}
