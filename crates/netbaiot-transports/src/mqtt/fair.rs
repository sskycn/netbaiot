use std::{
    future::{Future, poll_fn},
    task::Poll,
};

pub(super) enum Work<C, F, P> {
    Command(C),
    Frame(F),
    Packet(P),
}

/// Poll each ordinary source once in rotating order. With all sources ready, a
/// source waits at most two completed selections. Cancellation/deadlines remain
/// in the outer biased select and always run before ordinary work.
#[derive(Default)]
pub(super) struct FairWork {
    next: usize,
}
impl FairWork {
    pub async fn select<C, F, P>(
        &mut self,
        command: impl Future<Output = C>,
        frame: impl Future<Output = F>,
        packet: impl Future<Output = P>,
    ) -> Work<C, F, P> {
        tokio::pin!(command, frame, packet);
        poll_fn(|cx| {
            for offset in 0..3 {
                let index = (self.next + offset) % 3;
                let ready = match index {
                    0 => command.as_mut().poll(cx).map(Work::Command),
                    1 => frame.as_mut().poll(cx).map(Work::Frame),
                    _ => packet.as_mut().poll(cx).map(Work::Packet),
                };
                if ready.is_ready() {
                    self.next = (index + 1) % 3;
                    return ready;
                }
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ready_commands_frames_and_control_packets_rotate_with_a_fixed_bound() {
        let mut fair = FairWork::default();
        for index in 0..300 {
            let work = fair
                .select(
                    std::future::ready(()),
                    std::future::ready(()),
                    std::future::ready(()),
                )
                .await;
            assert!(matches!(
                (index % 3, work),
                (0, Work::Command(())) | (1, Work::Frame(())) | (2, Work::Packet(()))
            ));
        }
        let cancelled = tokio_util::sync::CancellationToken::new();
        cancelled.cancel();
        tokio::select! {
            biased;
            _ = cancelled.cancelled() => (),
            _ = fair.select(std::future::ready(()), std::future::ready(()), std::future::ready(())) => panic!("ordinary work bypassed cancellation"),
        }
    }

    #[tokio::test]
    async fn pending_sources_register_wakes_without_spinning() {
        let mut fair = FairWork::default();
        let notify = tokio::sync::Notify::new();
        let mut work = Box::pin(fair.select(
            std::future::pending::<()>(),
            std::future::pending::<()>(),
            notify.notified(),
        ));
        poll_fn(|cx| {
            assert!(work.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        notify.notify_one();
        assert!(matches!(work.await, Work::Packet(())));
    }
}
