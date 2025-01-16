package com.example.model

import java.time.Instant
import java.util.UUID

case class TransactionRow(
  id: UUID,
  orderId: String, // the same as OrderRow
  amount: BigDecimal,
  createdAt: Instant
)

object TransactionRow {

  // applying an order update may not result in transaction
  def fromOrderUpdate(state: OrderRow, updated: OrderRow): Option[TransactionRow] = Option.when(updated.filled > 0) {
    TransactionRow(
      id = UUID.randomUUID(), // generate some id for our transaction
      orderId = state.orderId,
      amount = updated.filled - state.filled,
      createdAt = Instant.now()
    )
  }
}
