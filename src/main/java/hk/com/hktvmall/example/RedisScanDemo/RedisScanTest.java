package hk.com.hktvmall.example.RedisScanDemo;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
public class RedisScanTest
{
	private static final Logger LOG = LoggerFactory.getLogger(RedisScanTest.class);

	final List<String> methods = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
	final List<String> cacheNames = methods.stream().map("database-%s"::formatted).collect(Collectors.toList());

	final List<String> sku = Stream.generate(UUID::randomUUID).limit(22_000).map(UUID::toString).collect(Collectors.toList());

	private final RedisTemplate<String, String> redisTemplate;
	private final ApplicationContext appContext;

	@Autowired
	public RedisScanTest(RedisTemplate<String, String> redisTemplate, ApplicationContext appContext)
	{
		this.redisTemplate = redisTemplate;
		this.appContext = appContext;
	}

	@PostConstruct
	private void setUp()
	{
		dumpData();
		testScanRandomKey();
	}


	@EventListener(ApplicationReadyEvent.class)
	public void shutdownWhenInitSuccess()
	{
		SpringApplication.exit(appContext, () -> 0);
	}

	private void dumpData()
	{
		LOG.info("Dumping data start");

		final AtomicLong currentSize = new AtomicLong();
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(() -> LOG.info("Dumping data, processed %d keys".formatted(currentSize.get())),
			0,
			10,
			TimeUnit.SECONDS);

		sku.parallelStream()
			// method
			.flatMap(e -> methods.stream().map(m -> "%s_%s".formatted(m, e)))
			// cache names
			.flatMap(e -> cacheNames.stream().map(cn -> "%s::%s".formatted(cn, e)))
			.map(e -> "%s_%s".formatted(e, methods.get((int) (Math.random() * methods.size()))))
			.forEach(e -> {
				redisTemplate.opsForValue().set(e, e);
				currentSize.incrementAndGet();
			});
		executor.shutdownNow();

		LOG.info("Dumping data end, total processed %d keys".formatted(currentSize.get()));
	}

	private void testScanRandomKey()
	{
		final String randomSku = sku.get((int) (Math.random() * sku.size()));
		final String pattern = "*::*%s*".formatted(randomSku);

		LOG.info("pattern = %s".formatted(pattern));

		final Set<String> scan = scan(pattern);
		LOG.info("scan result size = %d".formatted(scan.size()));

		final Set<String> keys = keys(pattern);
		LOG.info("keys result size = %d".formatted(keys.size()));

		final boolean equal = CollectionUtils.containsAll(scan, keys);

		LOG.info("scan === keys? %s".formatted(equal));
	}

	private Set<String> scan(String ptn)
	{
		LOG.info("start scan");
		long start = System.nanoTime();

		final ScanOptions scanOptions = ScanOptions.scanOptions().match(ptn).count(1000).build();

		final Cursor<byte[]> cursor = redisTemplate.execute((RedisCallback<Cursor<byte[]>>) connection -> connection.scan(
			scanOptions));

		final LinkedHashSet<String> ret = new LinkedHashSet<>();
		if (cursor != null)
		{
			cursor.forEachRemaining(e -> {
				final String k = new String(e, StandardCharsets.UTF_8);
				ret.add(k);
			});
		}
		long finish = System.nanoTime();
		long timeElapsed = (long) ((finish - start) / 10e6);
		LOG.info("end, elapsed = %s ms".formatted(timeElapsed));
		return ret;
	}

	private Set<String> keys(String ptn)
	{
		LOG.info("start key");
		long start = System.nanoTime();

		final Set<String> keys = redisTemplate.keys(ptn);

		long finish = System.nanoTime();
		long timeElapsed = (long) ((finish - start) / 10e6);
		LOG.info("end, elapsed = %s ms".formatted(timeElapsed));
		return keys;
	}
}
