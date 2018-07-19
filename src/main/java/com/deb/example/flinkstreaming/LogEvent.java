package com.deb.example.flinkstreaming;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogEvent {
	
	protected String id;
	protected String tenantId;
	protected String recordType;
	protected String eventCategory;
	protected String user;
	protected String machine;
	protected String result; 
	protected Integer eventCount;
	protected Integer dataBytes;
	protected String startTime;
	protected String time;
	protected long timeLong;

	
	public LogEvent() {
		super();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

	public String getRecordType() {
		return recordType;
	}

	public void setRecordType(String recordType) {
		this.recordType = recordType;
	}

	public String getEventCategory() {
		return eventCategory;
	}

	public void setEventCategory(String eventCategory) {
		this.eventCategory = eventCategory;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getMachine() {
		return machine;
	}

	public void setMachine(String machine) {
		this.machine = machine;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public Integer getEventCount() {
		return eventCount;
	}

	public void setEventCount(Integer eventCount) {
		this.eventCount = eventCount;
	}

	public Integer getDataBytes() {
		return dataBytes;
	}

	public void setDataBytes(Integer dataBytes) {
		this.dataBytes = dataBytes;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public long getTimeLong() {
		return timeLong;
	}

	public void setTimeLong(long timeLong) {
		this.timeLong = timeLong;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataBytes == null) ? 0 : dataBytes.hashCode());
		result = prime * result + ((eventCategory == null) ? 0 : eventCategory.hashCode());
		result = prime * result + ((eventCount == null) ? 0 : eventCount.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((machine == null) ? 0 : machine.hashCode());
		result = prime * result + ((recordType == null) ? 0 : recordType.hashCode());
		result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
		result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
		result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		result = prime * result + (int) (timeLong ^ (timeLong >>> 32));
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LogEvent other = (LogEvent) obj;
		if (dataBytes == null) {
			if (other.dataBytes != null)
				return false;
		} else if (!dataBytes.equals(other.dataBytes))
			return false;
		if (eventCategory == null) {
			if (other.eventCategory != null)
				return false;
		} else if (!eventCategory.equals(other.eventCategory))
			return false;
		if (eventCount == null) {
			if (other.eventCount != null)
				return false;
		} else if (!eventCount.equals(other.eventCount))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (machine == null) {
			if (other.machine != null)
				return false;
		} else if (!machine.equals(other.machine))
			return false;
		if (recordType == null) {
			if (other.recordType != null)
				return false;
		} else if (!recordType.equals(other.recordType))
			return false;
		if (result == null) {
			if (other.result != null)
				return false;
		} else if (!result.equals(other.result))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		if (tenantId == null) {
			if (other.tenantId != null)
				return false;
		} else if (!tenantId.equals(other.tenantId))
			return false;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (timeLong != other.timeLong)
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "LogEvent [id=" + id + ", tenantId=" + tenantId + ", recordType=" + recordType + ", eventCategory="
				+ eventCategory + ", user=" + user + ", machine=" + machine + ", result=" + result + ", eventCount="
				+ eventCount + ", dataBytes=" + dataBytes + ", startTime=" + startTime + ", time=" + time
				+ ", timeLong=" + timeLong + "]";
	}
	
	public String toShortStringOld() {
		String sep = "|";
		return tenantId +sep+ recordType +sep+ eventCategory +sep+ user+sep+ machine +sep+ result +sep+ eventCount +sep+ dataBytes +sep+ time +sep+ timeLong;
	}
	
	public String toShortString() {
		String sep = "|";
		String resultString = (result != null) ? result+sep : "";
		String startTimeString = (startTime != null) ? startTime+sep : "";
		return id +sep+ recordType +sep+ eventCategory +sep+ tenantId +sep+ user+sep+ machine +sep+ resultString + startTimeString + time;
	}
	
	//TODO return proper json string rather than some string
	public String toJson() {
		String sep = "|";
		String resultString = (result != null) ? result+sep : "";
		String startTimeString = (startTime != null) ? startTime+sep : "";
		return id +sep+ recordType +sep+ eventCategory +sep+ tenantId +sep+ user+sep+ machine +sep+ resultString + startTimeString + time;
	}
	
	public static Long convertToLong(String timestamp) {
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"));
//		return ZonedDateTime.parse(timestamp,formatter).toEpochSecond();
    	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		long millisSinceEpoch = LocalDateTime.parse(timestamp, formatter).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
		return millisSinceEpoch;
	}

}
