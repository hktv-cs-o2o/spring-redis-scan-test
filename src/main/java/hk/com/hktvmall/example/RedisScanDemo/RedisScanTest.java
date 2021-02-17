package hk.com.hktvmall.example.RedisScanDemo;

import org.apache.commons.collections4.CollectionUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
public class RedisScanTest
{
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
		sku.parallelStream()
			// method
			.flatMap(e -> methods.stream().map(m -> "%s_%s".formatted(m, e)))
			// cache names
			.flatMap(e -> cacheNames.stream().map(cn -> "%s::%s".formatted(cn, e)))
			.map(e -> "%s_%s".formatted(e, methods.get((int) (Math.random() * methods.size()))))
			.forEach(e -> {
				System.out.printf("Setting key: %s%n", e);
				redisTemplate.opsForValue().set(e, e);
			});
	}

	private void testScanRandomKey()
	{
		final String randomSku = sku.get((int) (Math.random() * sku.size()));
		final String pattern = "*::*%s*".formatted(randomSku);

		System.out.printf("pattern = %s%n", pattern);

		final Set<String> scan = scan(pattern);
		System.out.printf("scan result size = %d%n", scan.size());

		final Set<String> keys = keys(pattern);
		System.out.printf("keys result size = %d%n", keys.size());

		final boolean equal = CollectionUtils.containsAll(scan, keys);

		System.out.printf("scan === keys? %s%n", equal);
	}

	private Set<String> scan(String ptn)
	{
		System.out.println("start scan");
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
		System.out.printf("end, elapsed = %s ms%n", timeElapsed);
		return ret;
	}

	private Set<String> keys(String ptn)
	{
		System.out.println("start key");
		long start = System.nanoTime();

		final Set<String> keys = redisTemplate.keys(ptn);

		long finish = System.nanoTime();
		long timeElapsed = (long) ((finish - start) / 10e6);
		System.out.printf("end, elapsed = %s ms%n", timeElapsed);
		return keys;
	}
}
