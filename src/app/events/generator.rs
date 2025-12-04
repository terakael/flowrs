use std::time::{Duration, Instant};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use tokio::sync::mpsc::{channel, Receiver, Sender};

use crossterm::event;

use super::custom::FlowrsEvent;

// Poll interval when event generator is paused (while editor is open)
const EVENT_PAUSE_POLL_INTERVAL_MS: u64 = 100;

pub struct EventGenerator {
    pub _tick_rate: Duration,
    pub rx_event: Receiver<FlowrsEvent>,
    pub _tx_event: Sender<FlowrsEvent>,
    paused: Arc<AtomicBool>,
}

impl EventGenerator {
    pub fn new(tick_rate: u16) -> Self {
        let (tx_event, rx_event) = channel::<FlowrsEvent>(500);

        let tick_rate = Duration::from_millis(u64::from(tick_rate));
        let tx_event_thread = tx_event.clone();
        let paused = Arc::new(AtomicBool::new(false));
        let paused_clone = paused.clone();
        
        tokio::spawn(async move {
            let mut last_tick = Instant::now();
            loop {
                // If paused, sleep briefly and skip event polling
                if paused_clone.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(EVENT_PAUSE_POLL_INTERVAL_MS)).await;
                    continue;
                }
                
                let timeout = tick_rate
                    .checked_sub(last_tick.elapsed())
                    .unwrap_or_else(|| Duration::from_secs(0));
                if let Ok(true) = event::poll(timeout) {
                    // Check again if paused before reading (avoid race condition)
                    if !paused_clone.load(Ordering::Relaxed) {
                        if let Ok(ev) = event::read() {
                            let _ = tx_event_thread.send(FlowrsEvent::from(ev)).await;
                        }
                    }
                }
                if last_tick.elapsed() > tick_rate {
                    let _ = tx_event_thread.send(FlowrsEvent::Tick).await;
                    last_tick = Instant::now();
                }
            }
        });

        Self {
            _tick_rate: tick_rate,
            rx_event,
            _tx_event: tx_event,
            paused,
        }
    }

    pub async fn next(&mut self) -> Option<FlowrsEvent> {
        self.rx_event.recv().await
    }
    
    /// Pause event polling - stops reading from stdin
    /// Use this before opening an external editor to prevent consuming keystrokes
    pub fn pause(&self) {
        log::debug!("Pausing event generator");
        self.paused.store(true, Ordering::Relaxed);
    }
    
    /// Resume event polling
    pub fn resume(&self) {
        log::debug!("Resuming event generator");
        self.paused.store(false, Ordering::Relaxed);
    }
}
