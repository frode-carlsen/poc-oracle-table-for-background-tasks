package fc.db.tablequeue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class Utils {

	/**
	 * Thread factory which lets us configure name of pooled thread, and daemon
	 * flag.
	 */
	static class NamedThreadFactory implements ThreadFactory {
		private final AtomicInteger counter = new AtomicInteger(1);
		private final String prefix;
		private final boolean daemon;

		public NamedThreadFactory(String prefix, boolean daemon) {
			this.prefix = prefix;
			this.daemon = daemon;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, prefix + "_" + String.format("%03d", counter.getAndIncrement())); //$NON-NLS-1$ //$NON-NLS-2$
			t.setDaemon(daemon);
			return t;
		}
	}

	/** Writer error to string. */
	static String getLastError(Long id, String taskName, Exception e) {
		StringWriter sw = new StringWriter(4096);
		sw.write("Failed to Process task id=" + id + ", taskName=" + taskName + "\n"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();

		return sw.toString();
	}

	static synchronized String getJvmUniqueProcessName() {
		return ManagementFactory.getRuntimeMXBean().getName();
	}

}
