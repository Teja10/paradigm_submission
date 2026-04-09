//! Artemis-style engine for orchestrating collectors, models, strategies, and executors

pub mod types;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tracing::{error, info};

pub use types::{Action, CollectorMessageType, Event, ExecutorMessageType, PlaceOrderParams};

/// Type alias for a boxed stream of events
pub type CollectorStream<'a> = BoxStream<'a, Event>;

/// Collector trait - produces events from external sources
#[async_trait]
pub trait Collector: Send {
    /// Human-readable name for this collector
    fn name(&self) -> &'static str;

    /// Returns a stream of events (consumes self)
    async fn get_event_stream(self: Box<Self>) -> Result<CollectorStream<'static>>;
}

/// Strategy trait - processes events and produces actions
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Human-readable name for this strategy
    fn name(&self) -> &str;

    /// Returns the collector message types this strategy handles
    fn handles(&self) -> &[CollectorMessageType];

    /// Check if this strategy should process the given event
    fn should_handle(&self, event: &Event) -> bool {
        self.handles().contains(&event.message_type())
    }

    /// Initialize strategy state
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an event and return any resulting actions
    async fn process_event(&mut self, event: Event) -> Vec<Action>;
}

/// Model trait - processes events and produces derived events
///
/// Models sit between collectors and strategies, computing derived state
/// (like fair values) and emitting derived events that strategies can consume.
/// Models can also perform side effects (like logging) without emitting events.
#[async_trait]
pub trait Model: Send + Sync {
    /// Human-readable name for this model
    fn name(&self) -> &str;

    /// Returns the collector message types this model handles
    fn handles(&self) -> &[CollectorMessageType];

    /// Check if this model should process the given event
    fn should_handle(&self, event: &Event) -> bool {
        self.handles().contains(&event.message_type())
    }

    /// Initialize model state
    async fn sync_state(&mut self) -> Result<()>;

    /// Process an event and return any derived events
    async fn process_event(&mut self, event: Event) -> Vec<Event>;
}

/// Executor trait - executes actions
#[async_trait]
pub trait Executor: Send + Sync {
    /// Human-readable name for this executor
    fn name(&self) -> &str;

    /// Returns the executor message types this executor handles
    fn handles(&self) -> &[ExecutorMessageType];

    /// Check if this executor should process the given action
    fn should_handle(&self, action: &Action) -> bool {
        self.handles().contains(&action.message_type())
    }

    /// Execute an action
    async fn execute(&self, action: Action) -> Result<()>;
}

/// The main engine that orchestrates collectors, models, strategies, and executors
/// TODO: don't really like this external emitter pattern, should find a better way to do this.
pub struct Engine {
    collectors: Vec<Box<dyn Collector>>,
    models: Vec<Box<dyn Model>>,
    strategies: Vec<Box<dyn Strategy>>,
    executors: Vec<Box<dyn Executor>>,
    event_channel_capacity: usize,
    action_channel_capacity: usize,
    /// Pre-created event sender (for external emitters like CoinbaseOrderBook)
    event_tx: Option<broadcast::Sender<Event>>,
}

impl Engine {
    /// Create a new engine with default channel capacities
    pub fn new() -> Self {
        Self {
            collectors: Vec::new(),
            models: Vec::new(),
            strategies: Vec::new(),
            executors: Vec::new(),
            event_channel_capacity: 4096,
            action_channel_capacity: 4096,
            event_tx: None,
        }
    }

    /// Create event broadcast channel and return sender for external emitters
    ///
    /// Call this before `run()` if you have external event sources (like CoinbaseOrderBook)
    /// that need to emit events directly to the engine.
    pub fn create_event_channel(&mut self) -> broadcast::Sender<Event> {
        let (event_tx, _) = broadcast::channel::<Event>(self.event_channel_capacity);
        self.event_tx = Some(event_tx.clone());
        event_tx
    }

    /// Add a collector to the engine
    pub fn add_collector(mut self, collector: Box<dyn Collector>) -> Self {
        self.collectors.push(collector);
        self
    }

    /// Add a model to the engine
    pub fn add_model(mut self, model: Box<dyn Model>) -> Self {
        self.models.push(model);
        self
    }

    /// Add a strategy to the engine
    pub fn add_strategy(mut self, strategy: Box<dyn Strategy>) -> Self {
        self.strategies.push(strategy);
        self
    }

    /// Add an executor to the engine
    pub fn add_executor(mut self, executor: Box<dyn Executor>) -> Self {
        self.executors.push(executor);
        self
    }

    /// Run the engine
    pub async fn run(mut self) -> Result<()> {
        info!("Starting engine...");

        // Use pre-created event channel if available, otherwise create new one
        let event_tx = self.event_tx.take().unwrap_or_else(|| {
            let (tx, _) = broadcast::channel::<Event>(self.event_channel_capacity);
            tx
        });
        let (action_tx, _) = broadcast::channel::<Action>(self.action_channel_capacity);

        let mut tasks = JoinSet::new();

        // Sync all models
        for model in &mut self.models {
            info!("Syncing model: {}", model.name());
            model.sync_state().await?;
        }

        // Sync all strategies
        for strategy in &mut self.strategies {
            info!("Syncing strategy: {}", strategy.name());
            strategy.sync_state().await?;
        }

        // Spawn collector tasks
        for collector in self.collectors {
            let event_tx = event_tx.clone();
            let name = collector.name();

            tasks.spawn(async move {
                info!("Starting collector: {}", name);
                match collector.get_event_stream().await {
                    Ok(mut stream) => {
                        use futures::StreamExt;
                        while let Some(event) = stream.next().await {
                            if event_tx.send(event).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Collector {} failed: {}", name, e);
                    }
                }
                info!("Collector {} stopped", name);
            });
        }

        // Spawn model tasks
        // Models subscribe to events and can emit derived events back to the event channel
        for mut model in self.models {
            let mut event_rx = event_tx.subscribe();
            let event_tx = event_tx.clone();
            let name = model.name().to_string();

            tasks.spawn(async move {
                info!("Starting model: {}", name);
                loop {
                    match event_rx.recv().await {
                        Ok(event) => {
                            let derived_events = model.process_event(event).await;
                            for derived_event in derived_events {
                                if event_tx.send(derived_event).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            error!("Model {} lagged {} events", name, n);
                        }
                    }
                }
                info!("Model {} stopped", name);
            });
        }

        // Spawn strategy tasks
        for mut strategy in self.strategies {
            let mut event_rx = event_tx.subscribe();
            let action_tx = action_tx.clone();
            let name = strategy.name().to_string();

            tasks.spawn(async move {
                info!("Starting strategy: {}", name);
                loop {
                    match event_rx.recv().await {
                        Ok(event) => {
                            let actions = strategy.process_event(event).await;
                            for action in actions {
                                if action_tx.send(action).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            error!("Strategy {} lagged {} events", name, n);
                        }
                    }
                }
                info!("Strategy {} stopped", name);
            });
        }

        // Spawn executor tasks
        for executor in self.executors {
            let mut action_rx = action_tx.subscribe();
            let name = executor.name().to_string();

            tasks.spawn(async move {
                info!("Starting executor: {}", name);
                loop {
                    match action_rx.recv().await {
                        Ok(action) => {
                            if let Err(e) = executor.execute(action).await {
                                error!("Executor {} error: {}", name, e);
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            error!("Executor {} lagged {} actions", name, n);
                        }
                    }
                }
                info!("Executor {} stopped", name);
            });
        }

        info!("Engine running with {} tasks", tasks.len());

        // Wait for all tasks
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                error!("Task panicked: {}", e);
            }
        }

        info!("Engine stopped");
        Ok(())
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}
