use std::{time::{SystemTime, Duration}, thread, iter, collections::VecDeque}; 
extern crate timely; 
extern crate dynamic_scaling_mechanism;

use dynamic_scaling_mechanism::Control; 
use timely::{Configuration, dataflow::{InputHandle, ProbeHandle,
operators::{Input, Inspect, Probe, Partition}}, order::TotalOrder, progress::Timestamp};

use dynamic_scaling_mechanism::latency_operator::StatefulLatencyOperator;


fn main() { println!("Test started"); timely::execute(Configuration::Process(2), |worker| {

  // these results happen to be right, but aren't guaranteed. the system is at liberty to re-order
  // within a timestamp. let mut result = vec![(0, 0), (0, 2), (0, 6), (0, 12), (0, 20), (1, 1),
  // (1, 4), (1, 9), (1, 16), (1, 25)];

  let index = worker.index(); 
  let mut input = InputHandle::new(); 
  let mut control = InputHandle::new(); 
  let mut probe = ProbeHandle::new();

  worker.dataflow::<u64,_,_>(|scope| { 
    let input = scope.input_from::<u64>(&mut input);
    let control = scope.input_from::<Control>(&mut control);

    let (data_stream, latency_stream) = input.stateful_latency(
      &control,
      |k : &u64| k.clone(),
      "Simulate latency",
      move |cap, data, output| {
        for (time, d) in data.drain(..) {
          if index == 0 {
            thread::sleep(Duration::from_secs(d));
          } else {
            thread::sleep(Duration::from_secs(d / 2));
          }
          let t = &cap.delayed(&time);
          let mut session = output.session(&t); 
          session.give(d);
        }
      }
    );
    data_stream.probe_with(&mut probe);

    latency_stream
      .inspect(move |x| { 
        println!("Worker {:?} elapsed time: {:?}", index, x);
      });
  });

  for round in 0..10 { 
    input.send(round); 
    input.advance_to(round + 1); 
    while probe.less_than(input.time()) {
      worker.step(); 
    } 
  } 
  println!("Finished sending"); 
  worker.step(); 
  }).unwrap(); }

