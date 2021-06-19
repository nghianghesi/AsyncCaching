package asyncCaching.rest;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import asyncCaching.server.di.AsyncMemCache;

@RestController
public class AsyncCachingController {
	
	@Autowired
	AsyncMemCache asyncMemCache;
	
	@RequestMapping(method = RequestMethod.POST, value = "/cache/store/{key}/{expectedDuration}")
	public void store(@PathVariable UUID key, @PathVariable long expectedDuration, @RequestBody String data) throws Exception {
	    this.asyncMemCache.cache(key, data, expectedDuration);
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/cache/retrieve/{key}")
	public String retrieve(@PathVariable UUID key) throws Exception {
	    return this.asyncMemCache.retrieve(key);
	}	
	
	@RequestMapping(method = RequestMethod.GET, value = "/cache/remove/{key}")
	public void remove(@PathVariable UUID key) throws Exception {
	    this.asyncMemCache.retrieve(key);
	}
}
