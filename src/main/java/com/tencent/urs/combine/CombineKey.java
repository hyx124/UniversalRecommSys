package com.tencent.urs.combine;

import java.io.Serializable;

public class CombineKey implements Serializable{
	private static final long serialVersionUID = 8310482442855192113L;
	private final String groupId;
	private final Long cid;
	
	public String getGroupId() {
		return groupId;
	}
	
	public Long getCid() {
		return cid;
	}
	
	
	public CombineKey(String groupId,Long cid) {
		this.groupId = groupId;
		this.cid = cid;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CombineKey other = (CombineKey) obj;
		if (this.toString() == null) {
			if (other.toString() != null)
				return false;
		} else if (!this.toString().equals(other.toString()))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public String toString() {
		return groupId+"#"+cid;
	}

	
	public static void main(String[] args) {
	}



}
