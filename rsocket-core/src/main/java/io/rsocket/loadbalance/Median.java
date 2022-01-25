/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.loadbalance;

/** This implementation gives better results because it considers more data-point. */
class Median extends FrugalQuantile {

  public Median() {
    super(0.5, 1.0);
  }

  public synchronized void reset() {
    super.reset(0.5);
  }

  @Override
  public synchronized void insert(double x) {
    if (sign == 0) {
      estimate = x;
      sign = 1;
    } else {
      final double estimate = this.estimate;
      if (x > estimate) {
        greaterThanZero(x);
      } else if (x < estimate) {
        lessThanZero(x);
      }
    }
  }

  private void greaterThanZero(double x) {
    double estimate = this.estimate;

    step += sign;

    if (step > 0) {
      estimate += step;
    } else {
      estimate += 1;
    }

    if (estimate > x) {
      step += (x - estimate);
      estimate = x;
    }

    if (sign < 0) {
      step = 1;
    }

    sign = 1;

    this.estimate = estimate;
  }

  private void lessThanZero(double x) {
    double estimate = this.estimate;

    step -= sign;

    if (step > 0) {
      estimate -= step;
    } else {
      estimate--;
    }

    if (estimate < x) {
      step += (estimate - x);
      estimate = x;
    }

    if (sign > 0) {
      step = 1;
    }

    sign = -1;

    this.estimate = estimate;
  }

  @Override
  public String toString() {
    return "Median(v=" + estimate + ")";
  }
}
