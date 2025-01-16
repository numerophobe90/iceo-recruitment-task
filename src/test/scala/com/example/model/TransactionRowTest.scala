package com.example.model

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class TransactionRowTest extends AnyWordSpec with Matchers {

  "TransactionRow" when {
    "created from an order update" should {
      "have correct amount" in {
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          total = 0.8,
          filled = 0,
          createdAt = ts,
          updatedAt = ts
        )

        val update = order.copy(filled = 0.5)

        val transaction = TransactionRow.fromOrderUpdate(order, update)

        transaction.amount shouldBe 0.5
      }
    }
  }
}
