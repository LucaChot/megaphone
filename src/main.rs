use std::{time::Duration, thread}; 
extern crate timely; 
extern crate dynamic_scaling_mechanism;

use dynamic_scaling_mechanism::{Control, Bin, BIN_SHIFT, ControlInst}; 
use timely::{Configuration, dataflow::{InputHandle, ProbeHandle,
operators::{Input, Inspect, Probe, Broadcast, Feedback, ConnectLoop, Delay}}};

use dynamic_scaling_mechanism::latency_operator::StatefulLatencyOperator;
use dynamic_scaling_mechanism::regulator::RegulateOperator;


fn main() { println!("Test started"); timely::execute(Configuration::Process(2), |worker| {

  // these results happen to be right, but aren't guaranteed. the system is at liberty to re-order
  // within a timestamp. let mut result = vec![(0, 0), (0, 2), (0, 6), (0, 12), (0, 20), (1, 1),
  // (1, 4), (1, 9), (1, 16), (1, 25)];

  let index = worker.index(); 
  let mut input = InputHandle::new(); 
  let mut probe = ProbeHandle::new();

  worker.dataflow::<u64,_,_>(|scope| { 
    let input = scope.input_from::<u64>(&mut input);

    let (feedback_handle, feedback_stream) = scope.feedback(1 as u64);
    //let mut control_handle = InputHandle::new(); 
    //let control = scope.input_from::<Control>(&mut control_handle);

    let (data_stream, latency_stream, config) = input.stateful_latency(
      &feedback_stream.broadcast(),
      |k : &u64| k.clone(),
      "Simulate latency",
      move |cap, data, _ : &mut Bin<_,Vec<()>, _>, output| {
        for (time, d) in data.drain(..) {
          if index == 0 {
            //thread::sleep(Duration::from_secs(d));
          } else {
            //thread::sleep(Duration::from_secs(d / 2));
          }
          let t = &cap.delayed(&time);
          let mut session = output.session(&t); 
          session.give(d);
        }
      }
    );
  
    


    let control = latency_stream
      .delay_batch(|time| time + 1)
      .regulate_latency(config);

    control.connect_loop(feedback_handle);

    data_stream.probe_with(&mut probe);

    latency_stream
      .inspect(move |x| { 
        println!("Worker {:?} elapsed time: {:?}", index, x);
      });
  });

  for round in 0..10 { 
      if index == 0 {
          input.send(round);
      }
      input.advance_to(round + 1);
      while probe.less_than(input.time()) {
          worker.step();
      }
  }
  println!("Finished sending"); 
  worker.step(); 
  }).unwrap(); }

