package asyncCaching.rest;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {		
	private ExecutorService manageExecutor = Executors.newFixedThreadPool(10);
	private Random random = new Random();

	@RequestMapping(method = RequestMethod.GET, value = "/demo/dosomething")
	public Future<String> doSomeThing() {
		return CompletableFuture.supplyAsync( () -> {
			try {
				Thread.sleep(100 + random.nextInt(50));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "Do Some Thing Other" + random.nextInt();
		}, manageExecutor);
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/demo/dosomeotherthing")
	public Future<String> doSomeThingOther() {
		return CompletableFuture.supplyAsync( () -> {
			try {
				Thread.sleep(10 + random.nextInt(100));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return "Do Some Other Thing Other" + random.nextInt();
		}, manageExecutor);
	}
	
}
