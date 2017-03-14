package fc.db.tablequeue;

import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Task info describing the task to run, including error handling.
 * TODO: callbacks
 */
public class TaskInfo {

	private static final Pattern VALID_KEY_PATTERN = Pattern.compile("[a-zA-Z0-9_\\.]");

	private final String taskName;

	private Long id;
	private int priority;
	private LocalDateTime runAfterDateTime;
	private Properties props = new Properties();

	private int maxAttempts;

	TaskInfo(String taskName) {
		this.taskName = taskName;
	}

	public static Builder build(String taskName) {
		TaskInfo taskCreator = new TaskInfo(taskName);
		return taskCreator.new Builder();
	}

	public Long getId() {
		return id;
	}

	public String getTaskName() {
		return taskName;
	}

	public int getPriority() {
		return priority;
	}

	public String getPropertyValue(String key) {
		return props.getProperty(key);
	}

	public LocalDateTime getRunAfterDateTime() {
		return this.runAfterDateTime;
	}

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public class Builder {

		public Builder withId(Long id) {
			TaskInfo.this.id = id;
			return this;
		}

		public Builder withPriority(int priority) {
			TaskInfo.this.priority = priority;
			return this;
		}

		public Builder withAfterDateTime(LocalDateTime afterTime) {
			TaskInfo.this.runAfterDateTime = afterTime;
			return this;
		}

		public Builder withProperty(String key, String value) {
			if (!VALID_KEY_PATTERN.matcher(key).matches()) {
				throw new IllegalArgumentException("Invalid key:" + key);
			}
			TaskInfo.this.props.put(key, value);
			return this;
		}

		public Builder withMaxAttempts(int attempts) {
			TaskInfo.this.maxAttempts = attempts;
			return this;
		}

		Builder withProperties(Properties props) {
			TaskInfo.this.props.putAll(props);
			return this;
		}

		public TaskInfo build() {
			return TaskInfo.this;
		}

	}

	String getPropertiesAsString() throws IOException {
		if (props == null || props.isEmpty()) {
			return null;
		} else {
			StringWriter sw = new StringWriter(200);
			props.store(sw, null);
			return sw.toString();
		}

	}

}
