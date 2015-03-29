package com.shocktrade.resources

import com.ldaniels528.broadway.core.resources.IterableResource

/**
 * Quote Symbol Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class QuoteSymbolResource(name: String) extends IterableResource[String] {

  /**
   * Returns an option of the resource name
   * @return an option of the resource name
   */
  override def getResourceName: Option[String] = Some(name)

  override def iterator: Iterator[String] = {
    Seq("AAPL", "AMD", "AMZN", "GOOG", "IBM", "INTC", "MSFT", "SNE").iterator
  }

  override def toString = s"${getClass.getName}($name)"

}
