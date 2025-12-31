use crate::receiver::{ShredReceiver, UdpReader, SHRED_SIZE};
use crate::types::{ProcessedFecSets, ShredBytesMeta};
use socket2::{Domain, Socket, Type};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{io, mem};
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info};

const RECV_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB
// recvmsg 相关配置
#[cfg(target_os = "linux")]
const RECV_MMSG_MESSAGES: usize = 256; // 一次接收的消息数量

pub struct SocketReader {
    pub socket: Arc<Socket>,
}

impl SocketReader {
    pub fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        // UDP socket
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;

        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.set_recv_buffer_size(RECV_BUFFER_SIZE)?;
        socket.set_nonblocking(true)?;

        // Busy poll
        #[cfg(target_os = "linux")]
        {
            use libc::{setsockopt, SOL_SOCKET, SO_BUSY_POLL};
            unsafe {
                let busy_poll: libc::c_int = 50; // microseconds
                use std::os::unix::io::AsRawFd;
                let result = setsockopt(
                    socket.as_raw_fd(),
                    SOL_SOCKET,
                    SO_BUSY_POLL,
                    &busy_poll as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&busy_poll) as libc::socklen_t,
                );
                if result < 0 {
                    tracing::warn!("Failed to set SO_BUSY_POLL");
                }
            }
        }

        socket.bind(&bind_addr.into())?;
        info!("UDP receiver bound to {}", bind_addr);

        Ok(Self {
            socket: Arc::new(socket),
        })
    }

    pub fn run(
        self,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> anyhow::Result<Vec<JoinHandle<()>>> {
        // Spawn receiver threads
        let num_receivers = 1;
        info!("Starting {} network receiver workers", num_receivers);
        let mut handles = Vec::with_capacity(num_receivers);

        for i in 0..num_receivers {
            let socket = Arc::clone(&self.socket);
            let senders = senders.clone();
            let processed_fec_sets = Arc::clone(&processed_fec_sets);

            let handle = task::spawn_blocking(move || {
                if let Err(e) = Self::receive_loop(socket, senders, processed_fec_sets) {
                    error!("Reciever {} failed: {}", i, e);
                }
            });
            handles.push(handle);
        }
        Ok(handles)
    }

    fn receive_loop(
        socket: Arc<Socket>,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> anyhow::Result<()> {
        // 使用 recvmmsg（Linux）或普通 recv（非 Linux）
        #[cfg(target_os = "linux")]
        Self::receive_loop_mmsg(socket, senders, processed_fec_sets)?;

        #[cfg(not(target_os = "linux"))]
        Self::receive_loop_recv(socket, senders, processed_fec_sets)?;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn receive_loop_mmsg(
        socket: Arc<Socket>,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "metrics")]
        let mut last_channel_update = std::time::Instant::now();

        // 预分配多个缓冲区用于 recvmmsg
        let mut buffers: Vec<Vec<MaybeUninit<u8>>> = (0..RECV_MMSG_MESSAGES)
            .map(|_| vec![MaybeUninit::<u8>::uninit(); SHRED_SIZE])
            .collect();

        let mut iovecs: Vec<libc::iovec> = buffers
            .iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: SHRED_SIZE,
            })
            .collect();

        let mut msghdrs: Vec<libc::msghdr> = (0..RECV_MMSG_MESSAGES)
            .map(|_| unsafe { mem::zeroed() })
            .collect();

        for (i, msghdr) in msghdrs.iter_mut().enumerate() {
            msghdr.msg_iov = &mut iovecs[i];
            msghdr.msg_iovlen = 1;
        }

        let mut mmsg: Vec<libc::mmsghdr> = msghdrs
            .into_iter()
            .map(|msghdr| libc::mmsghdr {
                msg_hdr: msghdr,
                msg_len: 0,
            })
            .collect();

        use std::os::unix::io::AsRawFd;
        loop {
            hotpath::measure_block!("receive_mmsg_loop", {
                // 调用 recvmmsg
                let fd = socket.as_raw_fd();
                let num_received = unsafe {
                    libc::recvmmsg(
                        fd,
                        mmsg.as_mut_ptr(),
                        mmsg.len() as ::libc::c_uint,
                        libc::MSG_DONTWAIT,
                        std::ptr::null_mut(),
                    )
                };

                if num_received < 0 {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::WouldBlock {
                        continue;
                    }
                    error!("recvmmsg error: {}", err);
                    #[cfg(feature = "metrics")]
                    if let Some(metrics) = Metrics::try_get() {
                        metrics
                            .errors
                            .with_label_values(&["receiver", "socket_receive"])
                            .inc();
                    }
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }

                let num_received = num_received as usize;
                if num_received == 0 {
                    continue;
                }

                // let received_at_micros = SystemTime::now()
                //     .duration_since(UNIX_EPOCH)
                //     .unwrap()
                //     .as_micros() as u64;

                // 处理每个接收到的消息
                for i in 0..num_received {
                    let size = mmsg[i].msg_len as usize;
                    if size == 0 {
                        continue;
                    }

                    // SAFETY: recvmmsg 保证了前 `size` 个字节已初始化
                    let initialized_data = unsafe {
                        std::slice::from_raw_parts(buffers[i].as_ptr() as *const u8, size)
                    };

                    // callback(initialized_data)

                    if let Err(e) = ShredReceiver::process_shred(
                        initialized_data,
                        &senders,
                        &processed_fec_sets,
                        // &received_at_micros,
                    ) {
                        error!("Receiver failed to process shred: {}", e);
                        #[cfg(feature = "metrics")]
                        if let Some(metrics) = Metrics::try_get() {
                            metrics
                                .errors
                                .with_label_values(&["receiver", "process_shred"])
                                .inc();
                        }
                    }
                }
            });
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn receive_loop_recv(
        socket: Arc<Socket>,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "metrics")]
        let mut last_channel_update = std::time::Instant::now();
        // Pre-allocate buffer
        let mut buffer = vec![MaybeUninit::<u8>::uninit(); SHRED_SIZE];

        loop {
            match socket.recv(&mut buffer) {
                Ok(size) if size > 0 => {
                    // let received_at_micros = SystemTime::now()
                    //     .duration_since(UNIX_EPOCH)?
                    //     .as_micros() as u64;

                    // SAFETY: socket.recv() guarantees the first `size` bytes are initialized
                    let initialized_data =
                        unsafe { std::slice::from_raw_parts(buffer.as_ptr() as *const u8, size) };

                    // callback(initialized_data, received_at_micros);

                    if let Err(e) = ShredReceiver::process_shred(
                        initialized_data,
                        &senders,
                        &processed_fec_sets,
                        // &received_at_micros,
                    ) {
                        error!("Receiver failed to process shred: {}", e);
                    }
                }
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    error!("Socket receive error: {}", e);
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }
}
