package sql4j.memory

import sql4j.memory.page.{PageHeader, PageManager, PageOps}
import zio.test.{assertTrue, Spec, TestClock, TestEnvironment, ZIOSpecDefault}
import zio.{Duration, Scope, ZEnvironment, ZIO}

object MetricsReporterSpec extends ZIOSpecDefault:
		override def spec: Spec[TestEnvironment & Scope, Any] =
				suite("MetricsReporterSpec")(
						test("periodic reporter prints page & pool metrics and completes") {
								for
										pool <- ZIO.succeed(new MemoryPool(totalPages = 4))
										manager <- ZIO.succeed(new PageManager(pool))
										e1 <- ZIO.succeed(manager.newPage())
										e2 <- ZIO.succeed(manager.newPage())
										s1 <- ZIO.succeed(PageOps.insertRecord(e1.buffer, PageHeader(e1.buffer), Array.fill(1)(1.toByte)))
										s2 <- ZIO.succeed(PageOps.insertRecord(e2.buffer, PageHeader(e2.buffer), Array.fill(2)(2.toByte)))
										_ <- ZIO.succeed(PageOps.deleteRecord(e2.buffer, s2)) // create a hole
										reporter =
											ZIO.foreachDiscard(1 to 3) { _ =>
													ZIO.logInfo(s"[METRICS] ${manager.metrics()}") *>
														ZIO.sleep(Duration.fromMillis(150))
											}
										fiber <- reporter.fork
										_ <- TestClock.adjust(Duration.fromMillis(150 * 3))
										_ <- fiber.join
								yield assertTrue(manager.currentCount() == 2)
						}
				)