//! Ordering messages of the sub-protocols in `febft`.

use std::cmp::{
    PartialOrd,
    PartialEq,
    Ordering,
};
use std::collections::VecDeque;
use std::sync::atomic::AtomicI32;

use either::{
    Left,
    Right,
    Either,
};

use crate::bft::communication::message::StoredMessage;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use crate::bft::msg_log::PERIOD;

/// Represents a sequence number attributed to a client request
/// during a `Consensus` instance.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, Ord, Eq, PartialEq, Hash)]
pub struct SeqNo(i32);

///Represents a seq number for clients to safely use when working with concurrent requests
///Can be translated
pub struct ThreadSafeSeqNo(AtomicI32);

pub(crate) enum InvalidSeqNo {
    Small,
    Big,
}

impl From<u32> for SeqNo {
    #[inline]
    fn from(sequence_number: u32) -> SeqNo {
        const MAX: u32 = i32::MAX as u32;
        SeqNo((sequence_number % MAX) as i32)
    }
}

impl From<ThreadSafeSeqNo> for SeqNo {
    #[inline]
    fn from(seq_num: ThreadSafeSeqNo) -> SeqNo {
        seq_num.to_seq_no()
    }
}

impl From<SeqNo> for u32 {
    #[inline]
    fn from(sequence_number: SeqNo) -> u32 {
        sequence_number.0 as u32
    }
}

impl From<SeqNo> for usize {
    #[inline]
    fn from(sequence_number: SeqNo) -> usize {
        sequence_number.0 as usize
    }
}

impl From<SeqNo> for u64 {
    #[inline]
    fn from(sequence_number: SeqNo) -> u64 {
        sequence_number.0 as u64
    }
}

impl PartialOrd for SeqNo {
    fn partial_cmp(&self, other: &SeqNo) -> Option<Ordering> {
        Some(match self.index(*other) {
            Right(0) => Ordering::Equal,
            Left(InvalidSeqNo::Small) => Ordering::Less,
             _ => Ordering::Greater,
        })
    }
}

impl ThreadSafeSeqNo {

    pub const ZERO: Self = ThreadSafeSeqNo(AtomicI32::new(0));

    /// Increments the SeqNo
    #[inline]
    pub(crate) fn next(&self) -> SeqNo {
        SeqNo(self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    pub fn to_seq_no(&self) -> SeqNo {
        SeqNo(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }

}

impl SeqNo {
    /// Represents the first available sequence number.
    pub const ZERO: Self = SeqNo(0);

    /// Returns the following sequence number.
    #[inline]
    pub(crate) fn next(self) -> SeqNo {
        let (next, overflow) = (self.0).overflowing_add(1);
        SeqNo(if overflow { 0 } else { next })
    }

    /// Return an appropriate value to index the `TboQueue`.
    #[inline]
    pub(crate) fn index(self, other: SeqNo) -> Either<InvalidSeqNo, usize> {
        // TODO: add config param for these consts
        const OVERFLOW_THRES_POS: i32 = 10000;
        const OVERFLOW_THRES_NEG: i32 = -OVERFLOW_THRES_POS;
        const DROP_SEQNO_THRES: i32 = (PERIOD + (PERIOD >> 1)) as i32;

        let index = {
            let index = (self.0).wrapping_sub(other.0);
            if index < OVERFLOW_THRES_NEG || index > OVERFLOW_THRES_POS {
                // guard against overflows
                i32::MAX
                    .wrapping_add(index)
                    .wrapping_add(1)
            } else {
                index
            }
        };

        if index < 0 || index > DROP_SEQNO_THRES {
            // drop old messages or messages whose seq no. is too
            // large, which may be due to a DoS attack of
            // a malicious node
            Left(if index < 0 {
                InvalidSeqNo::Small
            } else {
                InvalidSeqNo::Big
            })
        } else {
            Right(index as usize)
        }
    }
}

/// Takes an internal queue of a `TboQueue` (e.g. the one used in the consensus
/// module), and pops a message.
pub fn tbo_pop_message<M>(
    tbo: &mut VecDeque<VecDeque<StoredMessage<M>>>,
) -> Option<StoredMessage<M>> {
    if tbo.is_empty() {
        None
    } else {
        tbo[0].pop_front()
    }
}

/// Takes an internal queue of a `TboQueue` (e.g. the one used in the consensus
/// module), and queues a message.
pub fn tbo_queue_message<M: Orderable>(
    curr_seq: SeqNo,
    tbo: &mut VecDeque<VecDeque<StoredMessage<M>>>,
    m: StoredMessage<M>,
) {
    let index = match m.message().sequence_number().index(curr_seq) {
        Right(i) => i,
        Left(_) => {
            // FIXME: maybe notify peers if we detect a message
            // with an invalid (too large) seq no? return the
            // `NodeId` of the offending node.
            //
            // NOTE: alternatively, if this seq no pertains to consensus,
            // we can try running the state transfer protocol
            return;
        },
    };
    if index >= tbo.len() {
        let len = index - tbo.len() + 1;
        tbo.extend(std::iter::repeat_with(VecDeque::new).take(len));
    }
    tbo[index].push_back(m);
}

/// Takes an internal queue of a `TboQueue` (e.g. the one used in the consensus
/// module), and drops messages pertaining to the last sequence number.
pub fn tbo_advance_message_queue<M>(
    tbo: &mut VecDeque<VecDeque<StoredMessage<M>>>,
) {
    match tbo.pop_front() {
        Some(mut vec) => {
            // recycle memory
            vec.clear();
            tbo.push_back(vec);
        },
        None => (),
    }
}

/// Represents any value that can be oredered.
pub trait Orderable {
    /// Returns the sequence number of this value.
    fn sequence_number(&self) -> SeqNo;
}
