pub type OneShotTx<T> = oneshot::Sender<T>;

pub type OneShotRx<T> = oneshot::Receiver<T>;

#[inline]
pub(super) fn channel<T>() -> (OneShotTx<T>, OneShotRx<T>) {

    oneshot::channel()

}