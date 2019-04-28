#pragma once

#include <memory>
#include "gtest/gtest.h"

namespace terrier {

class TerrierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // initialize loggers

  }

  void TearDown() override {
  }
};

}  // namespace terrier
