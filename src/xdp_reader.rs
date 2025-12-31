#[cfg(target_os = "linux")]
use crate::receiver::ShredReceiver;
#[cfg(target_os = "linux")]
use crate::types::{ProcessedFecSets, ShredBytesMeta};
#[cfg(target_os = "linux")]
use arrayvec::ArrayVec;
#[cfg(target_os = "linux")]
use aya::maps::{MapData, PerCpuValues, RingBuf};
#[cfg(target_os = "linux")]
use aya::programs::{Xdp, XdpFlags};
#[cfg(target_os = "linux")]
use aya::util::nr_cpus;
#[cfg(target_os = "linux")]
use aya::{include_bytes_aligned, Ebpf};
#[cfg(target_os = "linux")]
use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use tokio::io::unix::AsyncFd;
#[cfg(target_os = "linux")]
use tokio::sync::mpsc::Sender;
#[cfg(target_os = "linux")]
use tokio::task::JoinHandle;
#[cfg(target_os = "linux")]
use tracing::{error, info};

const PACKET_DATA_SIZE: usize = 1232;

#[cfg(target_os = "linux")]
pub struct XdpReader {
    reader: AsyncFd<RingBuf<MapData>>,
}

#[cfg(target_os = "linux")]
impl XdpReader {
    pub fn new(iface: &str, bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let mut bpf = Ebpf::load(include_bytes_aligned!("../turbine-ebpf-spy"))?;
        let program: &mut Xdp = bpf
            .program_mut("xdp_turbine_probe")
            .ok_or_else(|| anyhow::anyhow!("program not found"))?
            .try_into()?;
        program.load()?;
        program.attach(iface, XdpFlags::default())?;
        let nr_cpus = nr_cpus().map_err(|(_, error)| error)?;
        let mut turbine_port_map =
            aya::maps::PerCpuHashMap::<_, _, u8>::try_from(bpf.map_mut("TURBINE_PORTS").unwrap())?;
        let port = bind_addr.port();
        turbine_port_map.insert(port, PerCpuValues::try_from(vec![0; nr_cpus])?, 0)?;
        info!("Started watching turbine on {}", port);
        let turbine_packets = RingBuf::try_from(bpf.take_map("PACKET_BUF").unwrap())?;
        let reader = AsyncFd::new(turbine_packets)?;
        Ok(Self { reader })
    }

    pub fn run(
        self,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> anyhow::Result<Vec<JoinHandle<()>>> {
        let handle = tokio::spawn(async move {
            Self::receive_loop(self.reader, senders, processed_fec_sets).await
        });
        Ok(vec![handle])
    }

    async fn receive_loop(
        mut reader: AsyncFd<RingBuf<MapData>>,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) {
        loop {
            let mut guard = reader.readable_mut().await;
            let guard = match guard.as_mut() {
                Ok(x) => x,
                Err(err) => {
                    error!("Error reading from xdp: {}", err);
                    continue;
                }
            };
            let ring_buf = guard.get_inner_mut();
            // let received_at_micros = SystemTime::now()
            //     .duration_since(UNIX_EPOCH)
            //     .unwrap()
            //     .as_micros() as u64;
            while let Some(read) = ring_buf.next() {
                let ptr = read.as_ptr() as *const (ArrayVec<u8, PACKET_DATA_SIZE>, bool);
                let (data, _) = unsafe { core::ptr::read(ptr) };
                // println!("{}{}","收到udp包,长度: ",data.len());
                // callback(data.as_slice());

                let initialized_data = data.as_slice();
                if let Err(e) = ShredReceiver::process_shred(
                    initialized_data,
                    &senders,
                    &processed_fec_sets,
                    // &received_at_micros,
                ) {
                    error!("Receiver failed to process shred: {}", e);
                }
            }
            guard.clear_ready();
        }
    }
}
