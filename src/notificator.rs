use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;
use timely::logging::Logger;
use timely::ExchangeData;

/// Tracks requests for notification and delivers available notifications.
///
/// A `Notificator` represents a dynamic set of notifications and a fixed notification frontier.
/// One can interact with one by requesting notification with `notify_at`, and retrieving notifications
/// with `for_each` and `next`. The next notification to be delivered will be the available notification
/// with the least timestamp, with the implication that the notifications will be non-decreasing as long
/// as you do not request notifications at times prior to those that have already been delivered.
///
/// Notification requests persist across uses of `Notificator`, and it may help to think of `Notificator`
/// as a notification *session*. However, idiomatically it seems you mostly want to restrict your usage
/// to such sessions, which is why this is the main notificator type.
pub struct Notificator<'a, T: Timestamp, D: ExchangeData+Eq+PartialEq> {
    frontiers: &'a [&'a MutableAntichain<T>],
    inner: &'a mut FrontierNotificator<T, D>,
    logging: &'a Logger,
}

impl<'a, T: Timestamp, D: ExchangeData+Eq+PartialEq> Notificator<'a, T, D> {
    /// Allocates a new `Notificator`.
    ///
    /// This is more commonly accomplished using `input.monotonic(frontiers)`.
    pub fn new(
        frontiers: &'a [&'a MutableAntichain<T>],
        inner: &'a mut FrontierNotificator<T, D>,
        logging: &'a Logger) -> Self {

        inner.make_available(frontiers);

        Notificator {
            frontiers,
            inner,
            logging,
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> AntichainRef<T> {
        self.frontiers[input].frontier()
    }

    /// Requests a notification at the time associated with capability `cap`.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_content(data);
    ///                    let mut time = cap.time().clone();
    ///                    time.inner += 1;
    ///                    notificator.notify_at(cap.delayed(&time));
    ///                });
    ///                notificator.for_each(|cap,_,_| {
    ///                    println!("done with time: {:?}", cap.time());
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>, data: D) {
        self.inner.notify_at_frontiered(cap, data,self.frontiers);
    }

    /// Repeatedly calls `logic` until exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, Vec<D>, &mut Notificator<T, D>)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            self.logging.when_enabled(|l| l.log(::timely::logging::TimelyEvent::GuardedProgress(
                ::timely::logging::GuardedProgressEvent { is_start: true })));
            logic(cap, count, self);
            self.logging.when_enabled(|l| l.log(::timely::logging::TimelyEvent::GuardedProgress(
                ::timely::logging::GuardedProgressEvent { is_start: false })));
        }
    }
}

impl<'a, T: Timestamp, D: ExchangeData+Eq+PartialEq> Iterator for Notificator<'a, T, D> {
    type Item = (Capability<T>, Vec<D>);

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a capability for `t`, the timestamp being notified and, `count` represents
    /// how many notifications (out of those requested) are being delivered for that specific
    /// timestamp.
    #[inline]
    fn next(&mut self) -> Option<(Capability<T>, Vec<D>)> {
        self.inner.next(self.frontiers)
    }
}

/// Tracks requests for notification and delivers available notifications.
///
/// `FrontierNotificator` is meant to manage the delivery of requested notifications in the
/// presence of inputs that may have outstanding messages to deliver.
/// The notificator inspects the frontiers, as presented from the outside, for each input.
/// Requested notifications can be served only once there are no frontier elements less-or-equal
/// to them, and there are no other pending notification requests less than them. Each will be
/// less-or-equal to itself, so we want to dodge that corner case.
///
/// #Examples
/// ```
/// use std::collections::HashMap;
/// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::channels::pact::Pipeline;
///
/// timely::execute(timely::Configuration::Thread, |worker| {
///     let (mut in1, mut in2) = worker.dataflow(|scope| {
///         let (in1_handle, in1) = scope.new_input();
///         let (in2_handle, in2) = scope.new_input();
///         in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info| {
///             let mut notificator = FrontierNotificator::new();
///             let mut stash = HashMap::new();
///             move |input1, input2, output| {
///                 while let Some((time, data)) = input1.next() {
///                     stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
///                     notificator.notify_at(time.retain());
///                 }
///                 while let Some((time, data)) = input2.next() {
///                     stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
///                     notificator.notify_at(time.retain());
///                 }
///                 notificator.for_each(&[input1.frontier(), input2.frontier()], |time, _| {
///                     if let Some(mut vec) = stash.remove(time.time()) {
///                         output.session(&time).give_iterator(vec.drain(..));
///                     }
///                 });
///             }
///         }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
///
///         (in1_handle, in2_handle)
///     });
///
///     for i in 1..10 {
///         in1.send(i - 1);
///         in1.advance_to(i);
///         in2.send(i - 1);
///         in2.advance_to(i);
///     }
///     in1.close();
///     in2.close();
/// }).unwrap();
/// ```
pub struct FrontierNotificator<T: Timestamp, D: ExchangeData+Eq+PartialEq> {
    pending: Vec<(Capability<T>, Vec<D>)>,
    enqueued: Vec<(T, Vec<D>)>,
    available: ::std::collections::BinaryHeap<OrderReversed<T, D>>,
    capability: Option<Capability<T>>,
}

impl<T: Timestamp, D: ExchangeData+Eq+PartialEq> FrontierNotificator<T, D> {
    /// Allocates a new `FrontierNotificator`.
    pub fn new() -> Self {
        FrontierNotificator {
            pending: Vec::new(),
            enqueued: Vec::new(),
            available: ::std::collections::BinaryHeap::new(),
            capability: None,
        }
    }

    /// Allocates a new `FrontierNotificator` with initial capabilities.
    pub fn from<I: IntoIterator<Item=Capability<T>>>(iter: I) -> Self {
        FrontierNotificator {
            pending: iter.into_iter().map(|x| (x, vec![])).collect(),
            enqueued: Vec::new(),
            available: ::std::collections::BinaryHeap::new(),
            capability: None,
        }
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at(cap.delayed(&time));
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, _| {
    ///                        println!("done with time: {:?}", cap.time());
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at<'a>(&mut self, cap: Capability<T>, meta: Vec<D>) {
        self.pending.push((cap, meta));
    }

    /// Requests a notification at the time associated with capability `cap`.
    ///
    /// The method takes list of frontiers from which it determines if the capability is immediately available.
    /// When used with the same frontier as `make_available`, this method can ensure that notifications are
    /// non-decreasing. Simply using `notify_at` will only insert new notifications into the list of pending
    /// notifications, which are only re-examine with calls to `make_available`.
    #[inline]
    pub fn notify_at_frontiered<'a>(&mut self, cap: Capability<T>, data: D, frontiers: &'a [&'a MutableAntichain<T>]) {
        if frontiers.iter().all(|f| !f.less_equal(cap.time())) {
            self.available.push(OrderReversed::new(cap, vec![data]));
        } else {
            self.pending.push((cap, vec![data]));
        }
    }

    /// Enables pending notifications not in advance of any element of `frontiers`.
    pub fn make_available<'a>(&mut self, frontiers: &'a [&'a MutableAntichain<T>]) {

        // We can only reconstruct capabilities if we have one. This is true after `init_cap` has
        // been called.
        assert!(self.enqueued.is_empty() || self.capability.is_some(), "Notificator's capability needs to be initialized");

        // Move everything of `enqueued` to `pending` while converting the times to capabilities
        for (time, data) in self.enqueued.drain(..) {
            self.pending.push((self.capability.as_ref().unwrap().delayed(&time), data));
        }

        // Check if we can downgrade our capability.
        // Calculate lower bound of frontiers (TODO FIXME HACK - required total order!)
        let new_time = frontiers.iter().map(|f| f.frontier().iter().next().cloned()).flat_map(|c| c).min();
        // If the capability is less than the lower bound, downgrade to lower bound
        if new_time.as_ref().map_or(false, |t| self.capability.as_ref().unwrap().time() < t) {
            self.capability.as_mut().map(|c| c.downgrade(&new_time.unwrap()));
        }

        // Check if all frontiers are empty and drop our capability.
        if frontiers.iter().all(|f| f.frontier().is_empty()) {
            self.capability.take();
        }

        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.
//        if !self.pending.is_empty() {
//            println!("pending: {}", self.pending.len());
//        }

        if !self.pending.is_empty() {
            self.pending.sort_by(|x, y| x.0.time().cmp(y.0.time()));
            let mut i = 0;
            while i < self.pending.len() - 1 {
                let mut count = 0;
                {
                    let mut j = 1;
                    while i + j < self.pending.len() && self.pending[i].0.time() == self.pending[i + j].0.time() {
                        count += self.pending[i + j].1.len();
                        j += 1;
                    }
                }
                self.pending[i].1.reserve(count);
                {
                    let mut j = 1;
                    while i + j < self.pending.len() && self.pending[i].0.time() == self.pending[i + j].0.time() {
                        let data = ::std::mem::replace(&mut self.pending[i + j].1, vec![]);
                        self.pending[i].1.extend(data);
                        j += 1;
                    }
                    i += j;
                }
            }
            self.pending.retain(|x| x.1.len() > 0);

            for i in 0..self.pending.len() {
                if frontiers.iter().all(|f| !f.less_equal(&self.pending[i].0)) {
                    // TODO : This clones a capability, whereas we could move it instead.
                    let data = ::std::mem::replace(&mut self.pending[i].1, vec![]);
                    self.available.push(OrderReversed::new(self.pending[i].0.clone(), data));
                }
            }
            self.pending.retain(|x| x.1.len() > 0);
        }
    }

    /// Returns the next available capability with respect to the supplied frontiers, if one exists.
    ///
    /// In the interest of efficiency, this method may yield capabilities in decreasing order, in certain
    /// circumstances. If you want to iterate through capabilities with an in-order guarantee, either (i)
    /// use `for_each`
    #[inline]
    pub fn next<'a>(&mut self, frontiers: &'a [&'a MutableAntichain<T>]) -> Option<(Capability<T>, Vec<D>)> {
        if self.available.is_empty() {
            self.make_available(frontiers);
        }
        self.available.pop().map(|front| {
            while self.available.peek() == Some(&front) { self.available.pop(); }
            (front.element, front.data)
        })
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each<'a, F: FnMut(Capability<T>, Vec<D>, &mut FrontierNotificator<T, D>)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
        self.make_available(frontiers);
        while let Some((cap, data)) = self.next(frontiers) {
            logic(cap, data, self);
        }
    }

    /// Creates a notificator session in which delivered notification will be non-decreasing.
    ///
    /// This implementation can be emulated with judicious use of `make_available` and `notify_at_frontiered`,
    /// in the event that `Notificator` provides too restrictive an interface.
    #[inline]
    pub fn monotonic<'a>(&'a mut self, frontiers: &'a [&'a MutableAntichain<T>], logging: &'a Logger) -> Notificator<'a, T, D> {
        Notificator::new(frontiers, self, logging)
    }

    /// Iterates over pending capabilities and their count. The count represents how often a
    /// capability has been requested.
    ///
    /// To make sure all pending capabilities are above the frontier, use `for_each` or exhaust
    /// `next` to consume all available capabilities.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at(cap.delayed(&time));
    ///                        assert_eq!(notificator.pending().filter(|t| t.0.time() == &time).count(), 1);
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, _| {
    ///                        println!("done with time: {:?}", cap.time());
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    pub fn pending<'a>(&'a self) -> ::std::slice::Iter<'a, (Capability<T>, Vec<D>)> {
        self.pending.iter()
    }

    pub fn pending_mut<'a>(&'a mut self) -> &'a mut Vec<(Capability<T>, Vec<D>)> {
        &mut self.pending
    }

    pub fn enqueue(&mut self, time: T, data: Vec<D>) {
        self.enqueued.push((time, data));
    }

    pub fn init_cap(&mut self, cap: &Capability<T>) {
        if self.capability.is_none() {
            self.capability = Some(cap.clone());
        }
    }

}

#[derive(PartialEq, Eq)]
struct OrderReversed<T: Timestamp, D: Eq+PartialEq> {
    element: Capability<T>,
    data: Vec<D>,
}

impl<T: Timestamp, D: Eq+PartialEq> OrderReversed<T, D> {
    fn new(element: Capability<T>, data: Vec<D>) -> Self { OrderReversed { element, data } }
}

impl<T: Timestamp, D: Eq+PartialEq> PartialOrd for OrderReversed<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.element.time().partial_cmp(self.element.time())
    }
}
impl<T: Timestamp, D: Eq+PartialEq> Ord for OrderReversed<T, D> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.element.time().cmp(self.element.time())
    }
}
