//! General purpose migratable operators.

use std::collections::HashMap;
use std::time::{SystemTime, Duration};

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandle;
use timely::order::TotalOrder;

use ::{Bin, Control, Key, State};
use stateful::{Stateful, apply_state_updates, Notificator};
use notificator::{Notify};

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
    F: FnMut(&Capability<G::Timestamp>,
      &mut Vec<(G::Timestamp, D1)>,
      //&mut Bin<G::Timestamp, S, D1>,
      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
  >(&self, control: &Stream<G, Control>, key: B, name: &str, fold: F) -> (Stream<G, D2>, Stream<G,Duration>)
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
    F: FnMut(&Capability<G::Timestamp>,
      &mut Vec<(G::Timestamp, D1)>,
      //&mut Bin<G::Timestamp, S, D1>,
      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>) + 'static,    // state update logic
  >(&self, control: &Stream<G, Control>, key: B, name: &str, mut fold: F) -> (Stream<G, D2>, Stream<G,Duration>)  {

    let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

    let mut input = builder.new_input(self, Pipeline);
    //let mut input_state = builder.new_input(&stateful.state_stream, Exchange::new(move |&(target, _)| target as u64));

    let (mut data_output, data_stream) = builder.new_output();
    let (mut latency_output, latency_stream) = builder.new_output();


    let mut input_notificator = Notificator::new();
    let mut end_notificator = Notificator::new();
    
    let mut total_time = Duration::ZERO;
    let mut latency = HashMap::new();

    let mut input_drain = Vec::new();

    // TODO: Should probably be written in terms of `stateful_unary_input`
    builder.build(move |_capability| {
      move |frontiers| {
        let mut output_handle = data_output.activate();
        let mut latency_handle = latency_output.activate();

        while let Some((time, data)) = input.next() {
          if !latency.contains_key(time.time()) {
            latency.insert(time.time().clone(), total_time);
            end_notificator.notify_at(&time.delayed_for_output(time.time(),1));
          }
          let mut data_buffer = vec![];
          data.swap(&mut data_buffer);
          for d in data_buffer.drain(..){
            input_notificator.notify_at_data(&time.delayed(time.time()), time.time().clone(), d);
          }
        }

        let mut spent = Duration::ZERO;
        if let Some(cap) = input_notificator.drain(&[&frontiers[0]], &mut input_drain) {
          let start = SystemTime::now();
          fold(&cap, &mut input_drain, &mut output_handle);
          if let Ok(elapsed) = start.elapsed(){
            spent = elapsed;
          }
        }
        total_time += spent;

        end_notificator.for_each(&[&frontiers[0]], |cap, time, _|{
          let mut session = latency_handle.session(&cap);
          if let Some(start_time) = latency.get(&time) {
            session.give(total_time - start_time.clone());
          }
        });
      }
    });
    (data_stream, latency_stream)
  }
}
