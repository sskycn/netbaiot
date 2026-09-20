use crate::*;
use netbaiot_core::*;
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize)]
pub struct CommandDispatch {
    pub command_id: CommandId,
    pub state: DeliveryState,
}

pub struct CommandRouter {
    pub ingress: Arc<Ingress>,
}

impl CommandRouter {
    /// Commands are admitted only to a currently live local session. There is no
    /// offline queue and no persistence fallback.
    pub fn send(&self, command: DeviceCommand) -> Result<CommandDispatch> {
        let _gate = self.ingress.lifecycle.begin_admission()?;
        let now = now_ms();
        if command.expires_at <= now
            || command.expires_at.saturating_sub(now) > self.ingress.limits.command_ttl_ms as i64
        {
            return Err(Error::Invalid);
        }
        let endpoint = self
            .ingress
            .sessions
            .lookup(&command.device)?
            .ok_or(Error::Unavailable)?;
        if !endpoint.auth.permissions.commands {
            return Err(Error::Forbidden);
        }
        let bytes = self
            .ingress
            .codecs
            .get(&endpoint.auth)?
            .encode(
                &EncodeContext {
                    device: &command.device,
                },
                &command,
            )
            .map_err(|_| Error::Codec)?;
        if bytes.len() > self.ingress.limits.max_command_bytes {
            return Err(Error::Invalid);
        }
        endpoint.enqueue(&command, bytes).inspect_err(|_| {
            self.ingress.metrics.inc(Metric::QueueRejects);
        })?;
        self.ingress.metrics.inc(Metric::CommandQueued);
        Ok(CommandDispatch {
            command_id: command.command_id,
            state: DeliveryState::Queued,
        })
    }

    pub fn transport_state(&self, state: DeliveryState) {
        match state {
            DeliveryState::Sent => self.ingress.metrics.inc(Metric::CommandSent),
            DeliveryState::Received => self.ingress.metrics.inc(Metric::CommandReceived),
            DeliveryState::Failed | DeliveryState::Expired => {
                self.ingress.metrics.inc(Metric::CommandFailed);
            }
            _ => {}
        }
    }
}
