package com.example.model

import com.example.stream.HasPartitionKey

import java.time.Instant

case class OrderRow(
  orderId: String,
  market: String,
  total: BigDecimal,
  filled: BigDecimal, // state of completion of the order
  createdAt: Instant,
  updatedAt: Instant
) {
  def fulfilled: Boolean = total == filled // TODO: doesn't handle order "overflow"
}

object OrderRow {
  implicit val orderRowHasPartitonKey: HasPartitionKey[OrderRow] = (t: OrderRow) => t.orderId
}
