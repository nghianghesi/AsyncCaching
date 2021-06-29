package asyncCaching.rest;

import java.util.UUID;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import asyncCaching.server.di.AsyncMemCache;

@RestController
public class AsyncCachingController {
	private Logger logger = LoggerFactory.getLogger(AsyncCachingController.class);
	@Autowired
	AsyncMemCache asyncMemCache;
	
	@RequestMapping(method = RequestMethod.POST, value = "/cache/{key}/{expectedDuration}")
	public void store(@PathVariable UUID key, @PathVariable long expectedDuration, @RequestBody String data) throws Exception {
		this.logger.info("Store {}", key);  
		this.asyncMemCache.cache(key, data, expectedDuration);
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/cache/{key}")
	public Future<String> retrieve(@PathVariable UUID key) throws Exception {
		this.logger.info("Retrieve {}", key);
	    return this.asyncMemCache.retrieve(key);
	}	
	
	@RequestMapping(method = RequestMethod.DELETE, value = "/cache/{key}")
	public void remove(@PathVariable UUID key) throws Exception {
		this.logger.info("Remove {}", key);
	    this.asyncMemCache.retrieve(key);
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/cache/stats")
	public long stats() {
	    return this.asyncMemCache.size();
	}
}
