use std::{time::{SystemTime, Duration}, thread, iter, collections::VecDeque}; extern crate timely; extern crate dynamic_scaling_mechanism;

use dynamic_scaling_mechanism::notificator::TotalOrderFrontierNotificator; use
timely::{Configuration, dataflow::{InputHandle, ProbeHandle, channels::pact::Pipeline,
operators::{Input, Operator, Inspect, Probe, Partition}}, order::TotalOrder, progress::Timestamp};

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::{ConnectLoop, Filter, Map};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::Data;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::OutputHandle;

use dynamic_scaling_mechanism::{Bin, Control, Key, State};
//use dynamic_scaling_mechanism::stateful::{Stateful, apply_state_updates, Notificator};
use dynamic_scaling_mechanism::notificator::{Notify};

#[derive(Clone, Debug)]
pub enum LatencyStream<T> {
  Timestamp(SystemTime),
  Data(T)
}
pub trait LatencyOperator<G, D>
    where
        G: Scope,
        G::Timestamp: TotalOrder,
        D: ExchangeData + Eq,
{
    /// Stateful operator with a single input.
    fn latency 
      (&self, intervals: Box<dyn Iterator<Item=<G>::Timestamp>>) -> (Stream<G, D>, Stream<G,SystemTime>) ;
}

impl<G, D> LatencyOperator<G, D> for Stream<G, D>
    where
        G: Scope, // The containing scope
        G::Timestamp: TotalOrder,
        D: ExchangeData+Eq + std::fmt::Debug, // Input data
{
    /// Stateful operator with a single input and input transformation.

  fn latency
    (&self, mut intervals: Box<dyn Iterator<Item=<G>::Timestamp>>) -> (Stream<G, D>, Stream<G,SystemTime>) {
    let mut builder = OperatorBuilder::new(String::from("Latency"), self.scope());

    let mut data_in = builder.new_input(self, Pipeline);
    
    let (mut data_output, data_stream) = builder.new_output();
    let (mut latency_output, latency_stream) = builder.new_output();

    builder.build(move |mut _capability| {
      println!("Len {}",_capability.len());
      let mut notificator = TotalOrderFrontierNotificator::new();
      //let mut capability = Some(_capability);
      //if let Some(cap) = capability{
      //if let Some(interval) = intervals.next() {
        //let time = _capability[0].delayed(&interval);
        //notificator.notify_at(&time);
        //println!("To notify at {:?}", &time);
      //}
      //}

      let next_epoch = intervals.next();

      drop(_capability);

      //capability = None;

      move |frontiers| {
        let mut data_out = data_output.activate();
        let mut latency_output = latency_output.activate();
        let mut data_vec = vec![];

        data_in.for_each(|cap, data |{ 
          let mut session = data_out.session(&cap);
          //session.give_iterator(data.drain())
          data.swap(&mut data_vec);
          for d in data_vec.drain(..) {
            if let Some(interval) = next_epoch.clone() {
              if cap.time().eq(&interval) {
                notificator.notify_at(&cap.delayed(&interval));
              }

            }
            println!("Recieved {:?} at {:?}", &d, &cap);
            session.give(d);
          }
        });


        notificator.for_each(&[&frontiers[0]], |cap, time, notificator| { 
          let temp = cap.clone().delayed(&time);
          println!("Notified at {:?}", temp);
          let mut session = latency_output.session(&temp);
          let timestamp = SystemTime::now();
          println!("Start epoch {:?} at {:?} time", temp, timestamp);
          session.give(timestamp);
          if let Some(interval) = intervals.next() {
            let next = cap.delayed(&interval);
            notificator.notify_at(&next); 
            println!("To notify at {:?}", &next);
          }
        });
      }
    });
    (data_stream, latency_stream)
  }
}

pub trait ElapsedOperator<G, D>
    where
        G: Scope,
        G::Timestamp: TotalOrder,
        D: ExchangeData + Eq,
{
    /// Stateful operator with a single input.
    fn elapsed 
      (&self, latency_stream: &Stream<G,SystemTime>, intervals: Box<dyn Iterator<Item=<G>::Timestamp>>) -> Stream<G,Duration> ;
}

impl<G, D> ElapsedOperator<G, D> for Stream<G, D>
    where
        G: Scope, // The containing scope
        G::Timestamp: TotalOrder,
        D: ExchangeData+Eq + std::fmt::Debug, // Input data
{
    /// Stateful operator with a single input and input transformation.

  fn elapsed
    (&self, latency_stream: &Stream<G,SystemTime>, mut intervals: Box<dyn Iterator<Item=<G>::Timestamp>>) -> Stream<G,Duration> {

    self.binary_frontier(latency_stream, Pipeline, Pipeline, "Elapsed", |mut _capability, _info| {
      let mut notificator = TotalOrderFrontierNotificator::new();
      let mut capability = Some(_capability);
      if let Some(cap) = capability{
        if let Some(interval) = intervals.next() {
          let time = cap.delayed(&interval);
          notificator.notify_at(&time);
          println!("To notify at {:?}", &time);
        }
      }

      capability = None;
      
      let mut starts = VecDeque::new();
      move |input1, input2, output| {

        /*
        input1.for_each(|cap, data |{ 
          let mut session = output.session(&cap);
          //session.give_iterator(data.drain())
          data.swap(&mut data_vec);
          for d in data_vec.drain(..) {
            println!("Recieved {:?} at {:?}", &d, &cap);
            session.give(d);
          }
        });
        */
        while let Some((_, start)) = input2.next() {
          let mut data_vec = vec![];
          start.swap(&mut data_vec);

          for data in data_vec {
            starts.push_back(data)
          }
        }


        notificator.for_each(&[input1.frontier()], |cap, _, notificator| { 
          let mut session = output.session(&cap);
          let end = SystemTime::now();
          println!("Finish epoch {:?} at {:?} time", cap, end);
          if let Some(start) = starts.pop_front() {
            if let Ok(elapsed_time) = end.duration_since(start) {
              session.give(elapsed_time);
            }
          }

          if let Some(interval) = intervals.next() {
            let time = cap.delayed(&interval);
            notificator.notify_at(&time); 
            println!("To notify at {:?}", &time);
          }
        });
      }
    })
  }
}

//impl<T: timely::Data> timely::Data for LatencyStream<T> {}


fn main() { println!("Test started"); timely::execute(Configuration::Process(2), |worker| {

  // these results happen to be right, but aren't guaranteed. the system is at liberty to re-order
  // within a timestamp. let mut result = vec![(0, 0), (0, 2), (0, 6), (0, 12), (0, 20), (1, 1),
  // (1, 4), (1, 9), (1, 16), (1, 25)];

  let index = worker.index(); let mut input = InputHandle::new(); let mut probe =
    ProbeHandle::new();

  worker.dataflow::<u64,_,_>(|scope| { 
    let input = scope.input_from::<u64>(&mut input);
    let base = 1;
    let mut current = 0;
    let (data_stream, latency_stream) = input.latency(Box::from(iter::from_fn(move || {
      let result = current;
      current += base;
      Some(result)
    })));
    
    
    data_stream
      .inspect(move |x| { 
        println!("Waiting time: {:?}", x);
        thread::sleep(Duration::from_secs(*x));
      })
      .probe_with(&mut probe);
      //.elapsed(&latency_stream, Box::from(iter::from_fn(move || {
        ////let result = current;
        //current += base;
        //Some(result)
      //})))
     latency_stream 
      .inspect(move |x| { 
        println!("Elapsed time: {:?}", x);
      });
  });

  println!("Complete Graph"); 
  for round in 0..10 { 
    if index == 0 { 
      println!("Sent round: {}", round);
      input.send(round); 
    }
    input.advance_to(round + 1); 
    while probe.less_than(input.time()) {
      worker.step(); 
    } 
  } 
  println!("Finished sending"); 
  worker.step(); 
  panic!();

  }).unwrap(); }
