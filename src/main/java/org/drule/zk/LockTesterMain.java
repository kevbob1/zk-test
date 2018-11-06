package org.drule.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LockTesterMain {

	private static Logger logger = LoggerFactory.getLogger(LockTesterMain.class);

	private static final class DaemonTask implements Runnable {
		
		
		public DaemonTask() {
		}
		@Override
		public void run() {
			try {
				Thread.currentThread().join();
			} catch (InterruptedException e) {
				logger.error("interrupted", e);
			} catch (Exception e) {
				logger.error("lock didn't lock", e);
			}
		}
	}

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(LockTesterMain.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.setHeadless(true);
		app.run(args);
	}

	@Bean
	public CuratorFramework curatorClient() throws InterruptedException {

		final String zookeeperUrl = "localhost:2181";

		logger.info("Configuring ZooKeeperConfigSource using url: " + zookeeperUrl + ", applicationId: ");
		CuratorFramework cachedClient = CuratorFrameworkFactory.newClient(zookeeperUrl,
				new ExponentialBackoffRetry(1000, 3));
		cachedClient.start();
		cachedClient.blockUntilConnected();

		return cachedClient;
	}

	@Bean
	public Thread mainBlockerThread() {
		Thread thing = new Thread(new DaemonTask(), "MainBlocker");
		thing.start();
		return thing;
	}
	

	@Bean
	public LockTester lockTester(CuratorFramework curatorClient) {
		return new LockTester(curatorClient);
	}
}
