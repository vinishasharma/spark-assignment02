package edu.knoldus.application

import edu.knoldus.model.Operation

object ApplicationRun {
  def main(args: Array[String]): Unit = {
    Operation.getSalesRecords
  }
}

