/// Distributed epoch fencing token validator.
/// Rejects stale coordinators attempting writes after a cluster split.
pub struct FencingTokenValidator {
    current_epoch: u64,
}

impl FencingTokenValidator {
    pub fn new(initial_epoch: u64) -> Self {
        Self { current_epoch: initial_epoch }
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    pub fn validate_and_advance(&mut self, incoming_epoch: u64) -> bool {
        if incoming_epoch > self.current_epoch {
            self.current_epoch = incoming_epoch;
            true
        } else {
            false
        }
    }
}
