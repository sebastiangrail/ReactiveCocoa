//
//  Scheduler.swift
//  ReactiveCocoa
//
//  Created by Justin Spahr-Summers on 2014-06-02.
//  Copyright (c) 2014 GitHub. All rights reserved.
//

import Foundation

/// Represents a serial queue of work items.
public protocol SchedulerType {
	/// Enqueues an action on the scheduler.
	///
	/// When the work is executed depends on the scheduler in use.
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	func schedule(action: () -> Void) -> Disposable?
}

/// A particular kind of scheduler that supports enqueuing actions at future
/// dates.
public protocol DateSchedulerType: SchedulerType {
	/// The current date, as determined by this scheduler.
	///
	/// This can be implemented to deterministic return a known date (e.g., for
	/// testing purposes).
	var currentDate: NSDate { get }

	/// Schedules an action for execution at or after the given date.
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	func scheduleAfter(date: NSDate, action: () -> Void) -> Disposable?

	/// Schedules a recurring action at the given interval, beginning at the
	/// given start time.
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	func scheduleAfter(date: NSDate, repeatingEvery: TimeInterval, withLeeway: TimeInterval, action: () -> Void) -> Disposable?
}

/// A scheduler that performs all work synchronously.
public final class ImmediateScheduler: SchedulerType {
	public init() {}

	public func schedule(action: () -> Void) -> Disposable? {
		action()
		return nil
	}
}

/// A scheduler that performs all work on the main queue, as soon as possible.
///
/// If the caller is already running on the main queue when an action is
/// scheduled, it may be run synchronously. However, ordering between actions
/// will always be preserved.
public final class UIScheduler: SchedulerType {
//	private static var dispatchOnceToken: dispatch_once_t = 0
	private static var dispatchSpecificKey = DispatchSpecificKey<UInt8>()
	private static var dispatchSpecificContext: UInt8 = 0

	private var queueLength: Int32 = 0

	public init() {
//		dispatch_once(&UIScheduler.dispatchOnceToken) {
		DispatchQueue.main.setSpecific(key:UIScheduler.dispatchSpecificKey, value: UIScheduler.dispatchSpecificContext)
//		}
	}

	public func schedule(action: () -> Void) -> Disposable? {
		let disposable = SimpleDisposable()
		let actionAndDecrement = {
			if !disposable.disposed {
				action()
			}

			OSAtomicDecrement32(&self.queueLength)
		}

		let queued = OSAtomicIncrement32(&queueLength)

		// If we're already running on the main queue, and there isn't work
		// already enqueued, we can skip scheduling and just execute directly.
		if queued == 1 && DispatchQueue.getSpecific(key: UIScheduler.dispatchSpecificKey) == UIScheduler.dispatchSpecificContext {
			actionAndDecrement()
		} else {
			DispatchQueue.main.async(execute: actionAndDecrement)
		}

		return disposable
	}
}

/// A scheduler backed by a serial GCD queue.
public final class QueueScheduler: DateSchedulerType {
	internal let queue: DispatchQueue
	
	internal init(internalQueue: DispatchQueue) {
		queue = internalQueue
	}
	
	/// Initializes a scheduler that will target the given queue with its work.
	///
	/// Even if the queue is concurrent, all work items enqueued with the
	/// QueueScheduler will be serial with respect to each other.
	///
  	/// - warning: Obsoleted in OS X 10.11
	@available(OSX, deprecated:10.10, obsoleted:10.11, message:"Use init(qos:, name:) instead")
	public convenience init(queue: DispatchQueue, name: String = "org.reactivecocoa.ReactiveCocoa.QueueScheduler") {
		self.init(internalQueue: DispatchQueue(label: name, attributes: DispatchQueueAttributes.serial))
		self.queue.setTarget(queue: queue)
	}

	/// A singleton QueueScheduler that always targets the main thread's GCD
	/// queue.
	///
	/// Unlike UIScheduler, this scheduler supports scheduling for a future
	/// date, and will always schedule asynchronously (even if already running
	/// on the main thread).
	public static let mainQueueScheduler = QueueScheduler(internalQueue: DispatchQueue.main)
	
	public var currentDate: NSDate {
		return NSDate()
	}

	/// Initializes a scheduler that will target a new serial
	/// queue with the given quality of service class.
	@available(iOS 8, watchOS 2, OSX 10.10, *)
	public convenience init(qos: DispatchQoS.QoSClass = .background, name: String = "org.reactivecocoa.ReactiveCocoa.QueueScheduler") {
		// FIXME: qos now unused!
		let attributes = DispatchQueueAttributes(arrayLiteral: [DispatchQueueAttributes.serial, DispatchQueueAttributes.qosDefault])
		
		self.init(internalQueue: DispatchQueue(label: name, attributes: attributes))
	}

	public func schedule(action: () -> Void) -> Disposable? {
		let d = SimpleDisposable()

		queue.asynchronously() {
			if !d.disposed {
				action()
			}
		}

		return d
	}

	private func wallTimeWithDate(date: NSDate) -> DispatchWallTime {

		let (seconds, frac) = modf(date.timeIntervalSince1970)

		let nsec: Double = frac * Double(NSEC_PER_SEC)
		var walltime = timespec(tv_sec: Int(seconds), tv_nsec: Int(nsec))

		return DispatchWallTime(time: walltime)
	}

	public func scheduleAfter(date: NSDate, action: () -> Void) -> Disposable? {
		let d = SimpleDisposable()

		queue.after(walltime: wallTimeWithDate(date: date)) {
			if !d.disposed {
				action()
			}
		}

		return d
	}

	/// Schedules a recurring action at the given interval, beginning at the
	/// given start time, and with a reasonable default leeway.
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	public func scheduleAfter(date: NSDate, repeatingEvery: TimeInterval, action: () -> Void) -> Disposable? {
		// Apple's "Power Efficiency Guide for Mac Apps" recommends a leeway of
		// at least 10% of the timer interval.
		return scheduleAfter(date: date, repeatingEvery: repeatingEvery, withLeeway: repeatingEvery * 0.1, action: action)
	}

	public func scheduleAfter(date: NSDate, repeatingEvery: TimeInterval, withLeeway leeway: TimeInterval, action: () -> Void) -> Disposable? {
		precondition(repeatingEvery >= 0)
		precondition(leeway >= 0)

		let nsecInterval = repeatingEvery * Double(NSEC_PER_SEC)
		let nsecLeeway = leeway * Double(NSEC_PER_SEC)

		let timer = DispatchSource.timer(flags: [], queue: queue)
		timer.scheduleRepeating(
			wallDeadline: wallTimeWithDate(date: date),
			interval: DispatchTimeInterval.nanoseconds(Int(nsecInterval)),
			leeway: DispatchTimeInterval.nanoseconds(Int(nsecLeeway)))
		timer.setEventHandler(handler: action)
		timer.resume()

		return ActionDisposable {
			timer.cancel()
		}
	}
}

/// A scheduler that implements virtualized time, for use in testing.
public final class TestScheduler: DateSchedulerType {
	private final class ScheduledAction {
		let date: NSDate
		let action: () -> Void

		init(date: NSDate, action: () -> Void) {
			self.date = date
			self.action = action
		}

		func less(rhs: ScheduledAction) -> Bool {
			return date.compare(rhs.date as Date) == .orderedAscending
		}
	}

	private let lock = RecursiveLock()
	private var _currentDate: NSDate

	/// The virtual date that the scheduler is currently at.
	public var currentDate: NSDate {
		let d: NSDate

		lock.lock()
		d = _currentDate
		lock.unlock()

		return d
	}

	private var scheduledActions: [ScheduledAction] = []

	/// Initializes a TestScheduler with the given start date.
	public init(startDate: NSDate = NSDate(timeIntervalSinceReferenceDate: 0)) {
		lock.name = "org.reactivecocoa.ReactiveCocoa.TestScheduler"
		_currentDate = startDate
	}

	private func schedule(action: ScheduledAction) -> Disposable {
		lock.lock()
		scheduledActions.append(action)
		scheduledActions.sort { $0.less(rhs: $1) }
		lock.unlock()

		return ActionDisposable {
			self.lock.lock()
			self.scheduledActions = self.scheduledActions.filter { $0 !== action }
			self.lock.unlock()
		}
	}

	public func schedule(action: () -> Void) -> Disposable? {
		return schedule(action: ScheduledAction(date: currentDate, action: action))
	}

	/// Schedules an action for execution at or after the given interval
	/// (counted from `currentDate`).
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	public func scheduleAfter(interval: TimeInterval, action: () -> Void) -> Disposable? {
		return scheduleAfter(date: currentDate.addingTimeInterval(interval), action: action)
	}

	public func scheduleAfter(date: NSDate, action: () -> Void) -> Disposable? {
		return schedule(action: ScheduledAction(date: date, action: action))
	}

	private func scheduleAfter(date: NSDate, repeatingEvery: TimeInterval, disposable: SerialDisposable, action: () -> Void) {
		precondition(repeatingEvery >= 0)

		disposable.innerDisposable = scheduleAfter(date: date) { [unowned self] in
			action()
			self.scheduleAfter(date: date.addingTimeInterval(repeatingEvery), repeatingEvery: repeatingEvery, disposable: disposable, action: action)
		}
	}

	/// Schedules a recurring action at the given interval, beginning at the
	/// given interval (counted from `currentDate`).
	///
	/// Optionally returns a disposable that can be used to cancel the work
	/// before it begins.
	public func scheduleAfter(interval: TimeInterval, repeatingEvery: TimeInterval, withLeeway leeway: TimeInterval = 0, action: () -> Void) -> Disposable? {
		return scheduleAfter(date: currentDate.addingTimeInterval(interval), repeatingEvery: repeatingEvery, withLeeway: leeway, action: action)
	}

	public func scheduleAfter(date: NSDate, repeatingEvery: TimeInterval, withLeeway: TimeInterval = 0, action: () -> Void) -> Disposable? {
		let disposable = SerialDisposable()
		scheduleAfter(date: date, repeatingEvery: repeatingEvery, disposable: disposable, action: action)
		return disposable
	}

	/// Advances the virtualized clock by an extremely tiny interval, dequeuing
	/// and executing any actions along the way.
	///
	/// This is intended to be used as a way to execute actions that have been
	/// scheduled to run as soon as possible.
	public func advance() {
		advanceByInterval(interval: DBL_EPSILON)
	}

	/// Advances the virtualized clock by the given interval, dequeuing and
	/// executing any actions along the way.
	public func advanceByInterval(interval: TimeInterval) {
		lock.lock()
		advanceToDate(newDate: currentDate.addingTimeInterval(interval))
		lock.unlock()
	}

	/// Advances the virtualized clock to the given future date, dequeuing and
	/// executing any actions up until that point.
	public func advanceToDate(newDate: NSDate) {
		lock.lock()

		assert(currentDate.compare(newDate as Date) != .orderedDescending)
		_currentDate = newDate

		while scheduledActions.count > 0 {
			if newDate.compare(scheduledActions[0].date as Date) == .orderedAscending {
				break
			}

			let scheduledAction = scheduledActions.remove(at: 0)
			scheduledAction.action()
		}

		lock.unlock()
	}

	/// Dequeues and executes all scheduled actions, leaving the scheduler's
	/// date at `NSDate.distantFuture()`.
	public func run() {
		advanceToDate(newDate: NSDate.distantFuture())
	}
}
