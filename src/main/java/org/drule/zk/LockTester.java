
package org.drule.zk;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.ChildReaper;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockTester {
	private static Logger logger = LoggerFactory.getLogger(LockTester.class);

	public static final int MAX_THREADS = 10;

	private final CuratorFramework client;
	private ChildReaper reaper = null;

	public LockTester(CuratorFramework curatorClient) {
		this.client = curatorClient;
	}

	@PostConstruct
	public void start() throws Exception {
		for (int i = 1; i < (MAX_THREADS + 1); i++) {
			logger.info("starting lock tester: " + i);
			new Thread(new LockRunner(i)).start();
		}
		Set<String> schema = new HashSet<String>();
		schema.add("leases");
		schema.add("locks");

		reaper = new ChildReaper(client, "/locks", Reaper.Mode.REAP_UNTIL_DELETE,  ChildReaper.newExecutorService(), 5000, "/reaper/leader", schema );
		
		reaper.start();
	}
	
	@PreDestroy
	public void stop() throws Exception {
		
	}
	

	private class LockRunner implements Runnable {

		private final int num;
		private final Random rand = new Random();

		public LockRunner(int num) {
			this.num = num;
		}

		@Override
		public void run() {
			while (true) {
				try {
					// select random lock
					
					int lockNum = rand.nextInt(5000);
					InterProcessLock myLock = new InterProcessSemaphoreMutex(client, String.format("/locks/fert%d", lockNum));

					long start = System.currentTimeMillis();
					myLock.acquire();
					long stop = System.currentTimeMillis();
					logThing(String.format("acquire lock(%02d) delay=%d", lockNum, (stop - start)));

					Thread.sleep(3000);

					myLock.release();

					Thread.sleep(450);
				} catch (InterruptedException e) {
					logger.error("", e);
					break;
				} catch (Exception e) {
					logger.error("", e);
				}
			}
		}

		private void logThing(String msg_) {
			logger.info(String.format("[%02d] %s", num, msg_));
		}

	}

}