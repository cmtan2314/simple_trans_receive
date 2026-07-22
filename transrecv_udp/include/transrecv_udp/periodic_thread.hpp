#ifndef TRANSRECV_UDP__PERIODIC_THREAD_HPP_
#define TRANSRECV_UDP__PERIODIC_THREAD_HPP_

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <rclcpp/rclcpp.hpp>

namespace transrecv_udp
{

/// Runs one callback on a fixed period, on its own thread.
///
/// Deliberately not a `create_wall_timer`: every timer shares the executor, so
/// a slow callback delays all the others. Each job that must keep its own
/// cadence -- the 500 Hz publisher, the watchdog -- gets one of these instead.
///
/// The wait is a condition_variable, not a sleep, so `stop()` returns as soon
/// as the current pass finishes instead of waiting out the whole period.
/// Ownership is RAII: the destructor stops and joins, so the thread can never
/// outlive the object whose members the callback reads.
///
/// The period is a lower bound, not a guarantee: `wait_for` may overshoot under
/// load, and at a 2 ms period ordinary scheduler jitter is a visible fraction
/// of it. This is a best-effort rate, not a real-time one.
class PeriodicThread
{
public:
  /// `work` runs every `period` until stopped. It must not throw and must not
  /// outlive the state it captures -- keep this object a member of that state.
  PeriodicThread(
    std::chrono::milliseconds period,
    rclcpp::Logger logger,
    std::function<void()> work,
    std::string name)
  : period_(period),
    logger_(std::move(logger)),
    work_(std::move(work)),
    name_(std::move(name))
  {
  }

  ~PeriodicThread()
  {
    stop();
  }

  PeriodicThread(const PeriodicThread &) = delete;
  PeriodicThread & operator=(const PeriodicThread &) = delete;
  PeriodicThread(PeriodicThread &&) = delete;
  PeriodicThread & operator=(PeriodicThread &&) = delete;

  /// Spawns the worker. Call it only once the owning object is fully built --
  /// the thread starts reading captured state immediately.
  void start()
  {
    if (thread_.joinable()) {                     // Rule 3: double-start guard
      RCLCPP_WARN(logger_, "%s already running, ignoring start request", name_.c_str());
      return;
    }

    if (work_ == nullptr) {                       // Rule 3: null callable guard
      RCLCPP_ERROR(logger_, "%s has no callback, not starting", name_.c_str());
      return;
    }

    if (period_ <= std::chrono::milliseconds::zero()) {  // Rule 3: range guard
      RCLCPP_ERROR(
        logger_, "%s period %ldms is not positive, not starting",
        name_.c_str(), static_cast<long>(period_.count()));
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      running_ = true;
    }
    thread_ = std::thread([this]() { run(); });
    RCLCPP_INFO(
      logger_, "%s started (period %ldms)", name_.c_str(), static_cast<long>(period_.count()));
  }

  /// Signals the worker and joins it. Safe to call twice, and from the
  /// destructor after an explicit stop.
  void stop()
  {
    if (!thread_.joinable()) {
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      running_ = false;
    }
    cv_.notify_all();                             // wake now, don't wait out the period
    thread_.join();
    RCLCPP_INFO(logger_, "%s stopped", name_.c_str());
  }

  bool running() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_;
  }

private:
  void run()
  {
    for (;;) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, period_, [this]() { return !running_; });
        if (!running_) {
          return;
        }
      }

      if (!rclcpp::ok()) {                        // Rule 2: log why the loop ends
        RCLCPP_INFO(logger_, "%s: rclcpp shutting down, exiting", name_.c_str());
        return;
      }

      work_();
    }
  }

  const std::chrono::milliseconds period_;
  const rclcpp::Logger logger_;
  const std::function<void()> work_;
  const std::string name_;

  std::thread thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool running_ = false;
};

}  // namespace transrecv_udp

#endif  // TRANSRECV_UDP__PERIODIC_THREAD_HPP_
