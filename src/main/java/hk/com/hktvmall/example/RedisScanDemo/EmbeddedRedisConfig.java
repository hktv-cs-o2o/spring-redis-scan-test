package hk.com.hktvmall.example.RedisScanDemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Configuration;
import redis.embedded.RedisServer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


@Configuration
@AutoConfigureBefore(RedisAutoConfiguration.class)
public class EmbeddedRedisConfig
{
	private final RedisServer embeddedServer;

	@Autowired
	public EmbeddedRedisConfig(RedisProperties redisProperties)
	{
		embeddedServer = new RedisServer(redisProperties.getPort());
	}

	@PostConstruct
	private void setUp()
	{
		embeddedServer.start();
	}

	@PreDestroy
	private void tearDown()
	{
		embeddedServer.stop();
	}
}
