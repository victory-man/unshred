use crate::receiver::{RECV_MMSG_MESSAGES, SHRED_SIZE};
use crossbeam::queue::ArrayQueue;
use std::mem::MaybeUninit;

pub struct BufferAllocator {
    size: usize,
    point: usize,
    queue: ArrayQueue<Vec<Vec<MaybeUninit<u8>>>>,
}

impl BufferAllocator {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            point: size / 2,
            queue: ArrayQueue::new(size),
        }
    }

    pub fn fill(&self) -> bool {
        let remain = self.queue.len();
        if remain > self.point {
            return true;
        }
        for _ in 0..self.size - remain {
            let mut buffer = Vec::with_capacity(RECV_MMSG_MESSAGES);
            for _ in 0..RECV_MMSG_MESSAGES {
                buffer.push(vec![MaybeUninit::<u8>::uninit(); SHRED_SIZE]);
            }

            if let Err(_) = self.queue.push(buffer) {
                return true;
            }
        }
        false
    }

    pub fn reuse_buffer(&self, buf: Vec<Vec<MaybeUninit<u8>>>) {
        let _ = self.queue.push(buf);
    }

    pub fn allocate(&self) -> Vec<Vec<MaybeUninit<u8>>> {
        self.queue.pop().unwrap_or_else(|| {
            let mut buffer = Vec::with_capacity(RECV_MMSG_MESSAGES);
            for _ in 0..RECV_MMSG_MESSAGES {
                buffer.push(vec![MaybeUninit::<u8>::uninit(); SHRED_SIZE]);
            }
            buffer
        })
    }
}
