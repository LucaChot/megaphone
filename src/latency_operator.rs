//! General purpose migratable operators.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, Duration};

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope, ScopeParent};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::{Capability, Filter, Map, ConnectLoop};
use timely::dataflow::operators::generic::OutputHandle;
use timely::order::TotalOrder;

use ::{Bin, Control};
use stateful::{apply_state_updates, Notificator};
use notificator::Notify;

use crate::ControlSet;
use crate::map_stateful::MapStateful;

/// Building blocks for single- and dual-input stateful operators.
pub trait StatefulLatencyOperator<G, D1>
  where
    G: Scope,
    G::Timestamp: TotalOrder,
    D1: ExchangeData + Eq,
{
  /// Stateful operator with a single input.
  fn stateful_latency<
    D2: Data,                                    // output type
    B: Fn(&D1)->u64+'static,
    S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
    W: ExchangeData,                            // State format on the wire
    F: FnMut(&Capability<G::Timestamp>,
      &mut Vec<(G::Timestamp, D1)>,
      &mut Bin<G::Timestamp, S, D1>,
      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
  >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> (Stream<G, D2>, Stream<G,(u64,Duration)>, Rc<RefCell<ControlSet<<G as ScopeParent>::Timestamp>>>) 
  ;

}

impl<G, D1> StatefulLatencyOperator<G, D1> for Stream<G, D1>
  where
    G: Scope, // The containing scope
    G::Timestamp: TotalOrder,
    D1: ExchangeData+Eq, // Input data
{
  fn stateful_latency<
    D2: Data,                                    // output type
    B: Fn(&D1)->u64+'static,
    S: Clone+IntoIterator<Item=W>+Extend<W>+Default+'static,
    W: ExchangeData,                            // State format on the wire
    F: FnMut(&Capability<G::Timestamp>,
      &mut Vec<(G::Timestamp, D1)>,
      &mut Bin<G::Timestamp, S, D1>,
      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
  >(&self, control: &Stream<G, Control>, key: B, name: &str, mut fold: F) -> (Stream<G, D2>, Stream<G,(u64,Duration)>, Rc<RefCell<ControlSet<<G as ScopeParent>::Timestamp>>>)  {

    let index = self.scope().index() as u64;

    let stateful = self.map_stateful(key, control);
    let states = stateful.state.clone();
    let config = stateful.config.clone();

    let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
    let mut input = builder.new_input(&stateful.stream, Exchange::new(move |&(target, _key, _)| target as u64));
    let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

    let (mut output, stream) = builder.new_output();
    let (mut latency_output, latency_stream) = builder.new_output();

    let mut state_update_buffer = vec![];

    let mut notificator = Notificator::new();

    let mut end_notificator = Notificator::new();
    
    let mut total_time = Duration::ZERO;
    let mut latency = HashMap::new();

    let mut not_drain = Vec::new();
    let mut bin_drain = Vec::new();

    // TODO: Should probably be written in terms of `stateful_unary_input`
    builder.build(move |_capability| {
        move |frontiers| {
            let mut output_handle = output.activate();
            let mut latency_handle = latency_output.activate();

            let mut states = states.borrow_mut();
            while let Some((time, data)) = input_state.next() {
                data.swap(&mut state_update_buffer);
                apply_state_updates(&mut states, &time.retain(), state_update_buffer.drain(..))
            }
            // stash each input and request a notification when ready
            while let Some((time, data)) = input.next() {
                if !latency.contains_key(time.time()) {
                    latency.insert(time.time().clone(), total_time);
                    end_notificator.notify_at(&time.delayed_for_output(time.time(),1));
                }
                let mut data_buffer = vec![];
                data.swap(&mut data_buffer);
                let cap = time.retain();
                notificator.notify_at_data(&cap, cap.time().clone(), data_buffer);
            }

            if let Some(cap) = notificator.drain(&[&frontiers[0], &frontiers[1]], &mut not_drain) {
                for (time, mut keyed_data) in not_drain.drain(..) {
                    for (_, key_id, d) in keyed_data.drain(..) {
                        states.get(key_id).notificator.notify_at_data(&cap, time.clone(), d);
                    }
                }
            }

            // go through each time with data
            let mut spent = Duration::ZERO;
            for bin in states.bins.iter_mut().filter(|b| b.is_some()) {
                let bin = bin.as_mut().unwrap();
                if let Some(cap) = bin.notificator().drain(&[&frontiers[0], &frontiers[1]], &mut bin_drain) {
                    let start = SystemTime::now();
                    fold(&cap, &mut bin_drain, bin, &mut output_handle);
                    if let Ok(elapsed) = start.elapsed(){
                        spent = elapsed;
                    }
                }
            }
            total_time += spent;


            end_notificator.for_each(&[&frontiers[0]], |cap, time, _|{
                let mut session = latency_handle.session(&cap);
                if let Some(start_time) = latency.get(&time) {
                    session.give((index, total_time - start_time.clone()));
                }
            });
        }
    });
    let progress_stream = stream.filter(|_| false).map(|_| ());
    progress_stream.connect_loop(stateful.feedback);
    (stream, latency_stream, config)
  }
}
