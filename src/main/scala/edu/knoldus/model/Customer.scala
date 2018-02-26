package edu.knoldus.model

case class Customer(id: String,
                    name: String,
                    address: String,
                    zip: String) {
  val city = address.split(' ').last

}