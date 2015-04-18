package com.ldaniels528.broadway.core.util

import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

import scala.concurrent.duration._

/**
 * Counter Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CounterSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a Counter")
  info("I want to be able to track the performance of an I/O-bound process")

  feature("Produce the current I/O rate once per second") {
    scenario("Simulate a process updating the counter") {
      Given("a counter instance")
      val counter = new Counter(1.minute)((delta, errors, rps) => info(f"Input -> Output: $delta records ($rps%.1f records/second)"))

      When("Once the counter has collected data for at least 1 minute")
      val startTime = System.currentTimeMillis()
      while (System.currentTimeMillis() - startTime < 61000) {
        counter += 1
      }

      Then("The counter should have displayed a rate")
      info(s"The counter reads $counter")
    }
  }

}
