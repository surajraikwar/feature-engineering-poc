package com.example.featureplatform

class MainAppSpec extends AnyFlatSpec with Matchers {

  "A simple assertion" should "be true" in {
    assert(1 == 1)
  }

  it should "also be true with matchers" in {
    1 shouldBe 1
  }
}
