package:
	@docker run -it --rm -v ${PWD}:/app -v ~/.ivy2:/root/.ivy2 rxvc/scala-sbt sbt package

clean:
	@docker run -it --rm -v ${PWD}:/app -v ~/.ivy2:/root/.ivy2 rxvc/scala-sbt sbt clean

test:
	@docker run -it --rm -v ${PWD}:/app -v ~/.ivy2:/root/.ivy2 rxvc/scala-sbt sbt test
