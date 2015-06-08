package io.segment.benchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.segment.analytics.Analytics;
import com.segment.analytics.messages.TrackMessage;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import com.google.common.util.concurrent.RateLimiter;

public class JavaClientBenchmark {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		runTest(50, 60000, 1000);
	}

	public static void runTest(int requestsPerSecond, int durationMs, int sampleTime) {

		Sigar sigar = new Sigar();

		String filename = "benchmark" + (int)Math.floor(Math.random() * 1000) + ".csv";

		FileWriter writer = null;
		try {
			writer = new FileWriter(filename);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		final Analytics analytics =
				Analytics.builder("testsecret").flushInterval(5000, TimeUnit.MILLISECONDS).build();

		RateLimiter rate = RateLimiter.create(requestsPerSecond);

		long start = System.currentTimeMillis();
		long lastSample = 0;

		double insertTime = 0;
		int count = 0;

		System.out.println("Test starting with filename " + filename + " ...");

		String headings = "CPU,Memory,Insert Time (avg ms)";

		System.out.println(headings);

		try {
			writer.write(headings + "\n");
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while(System.currentTimeMillis() - start <= durationMs) {

			if (rate.tryAcquire()) {
				String userId = "benchmark" + (int)Math.floor(Math.random() * 1000);

				long insertStart = System.currentTimeMillis();

				analytics.enqueue(TrackMessage.builder("Benchmark")
								.userId(userId)
								.properties(new HashMap<String, Object>() {{
									put("name", "Achilles");
									put("shippingMethod", "3-day");
								}})
				);

				// analytics.flush();

				long insertDuration = System.currentTimeMillis() - insertStart;
				insertTime += insertDuration;
				count += 1;
			}

			if (lastSample == 0 || System.currentTimeMillis() - lastSample >= sampleTime) {
				// its time to take a sample

				try {
					System.gc();

					double cpuUsed = sigar.getCpuPerc().getCombined();
					long memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

					String line = cpuUsed + "," + memoryUsed +
							"," + (insertTime / count);

					System.out.println(line);

					try {
						writer.write(line + "\n");
						writer.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}

				} catch (SigarException e) {
					e.printStackTrace();
				}

				lastSample = System.currentTimeMillis();
			}
		}

		System.out.println("Test finished.");

		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		analytics.shutdown();
	}
}
