//! General purpose migratable operators.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use rand::{thread_rng, Rng};

use timely::dataflow::{Stream, Scope, InputHandle, ScopeParent};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::order::TotalOrder;

use ::Control;
use stateful::Notificator;
use notificator::Notify;
use crate::{ControlSet, ControlInst, BinId, BIN_SHIFT};

/// Building blocks for single- and dual-input stateful operators.
pub trait RegulateOperator<G>
  where
    G: Scope, // The containing scope
    G::Timestamp: TotalOrder,
{
  /// Stateful operator with a single input.
  fn regulate_latency<
  >(&self, config : Rc<RefCell<ControlSet<<G as ScopeParent>::Timestamp>>>) -> Stream<G, Control>
  ;

}

impl<G> RegulateOperator<G> for Stream<G, (u64, Duration)>
  where
    G: Scope, // The containing scope
    G::Timestamp: TotalOrder,
{
    fn regulate_latency<
        >(&self, config : Rc<RefCell<ControlSet<<G as ScopeParent>::Timestamp>>>) -> Stream<G, Control> {
        
        let index = self.scope().index();
        let mut rng = thread_rng();
        let peers = self.scope().peers();
        let mut latency : Vec<Duration> = (0..peers).map(|_| Duration::ZERO).collect();

        let mut builder = OperatorBuilder::new(String::from("Regulator"), self.scope());
        let mut input = builder.new_input(self, Exchange::new(move |_| 0));
        let (mut output, stream) = builder.new_output();

        let mut initial_notificator = Notificator::new();
        let mut notificator = Notificator::new();
        let mut not_drain = Vec::new();

        let mut sequence_num = 0;

        // TODO: Should probably be written in terms of `stateful_unary_input`
        builder.build(move |_capability| {
            if index == 0 {
                initial_notificator.notify_at(&_capability[0]);
            }

            move |frontiers| {
                let mut output_handle = output.activate();

                initial_notificator.for_each(&[&frontiers[0]], |cap, _, _|{
                    let mut session = output_handle.session(&cap);
                    session.give(Control::new(sequence_num,  1, ControlInst::Map(vec![0; 1 << BIN_SHIFT])));
                    sequence_num += 1;
                });

                // stash each input and request a notification when ready
                while let Some((time, data)) = input.next() {
                    let mut data_buffer = vec![];
                    data.swap(&mut data_buffer);
                    let cap = time.retain();
                    notificator.notify_at_data(&cap, cap.time().clone(), data_buffer);
                }

                if let Some(cap) = notificator.drain(&[&frontiers[0]], &mut not_drain) {
                    let mut latest = cap.time().clone();
                    for (time, mut durations) in not_drain.drain(..) {
                        if time > latest {
                            latest = time;
                        }
                        for (worker, duration) in durations.drain(..) {
                            latency[worker as usize] += duration;
                        }
                    }
                    if let Some(min) = latency.iter().zip(0..peers).min_by_key(|(_, x)| *x) {
                        let cap = cap.delayed(&latest);
                        let mut session = output_handle.session(&cap);
                        let active_config = config.borrow();
                        let num_bins = active_config.map().len();
                        let bins : Vec<_> = active_config.map().iter().zip(0..num_bins).filter(|(&worker, _bin)| worker == min.1).take((0.1 * (num_bins as f32)) as usize).collect();
                        let others : Vec<usize> = (0..peers).filter(|&x| x != min.1).collect();

                        let mut expected = bins.len();
                        for (_, bin) in bins {
                            let instr = match rng.choose(&others){
                                Some(&new_worker) => ControlInst::Move(BinId(bin), new_worker),
                                None => ControlInst::None
                            };

                            session.give(Control::new(sequence_num, expected, instr));
                            expected -= 1;
                        }
                        sequence_num += 1;
                    }
                }
            }
        });
        stream
    }
}
