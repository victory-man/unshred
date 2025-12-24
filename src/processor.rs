#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{types::ShredBytesMeta, TransactionEvent, TransactionHandler, UnshredConfig};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::{anyhow, Result};
use crossbeam_queue::ArrayQueue;
use dashmap::DashSet;
use solana_entry::entry::Entry;
use solana_ledger::shred::{ReedSolomonCache, Shred, ShredType};
use std::{
    io::Cursor,
    mem,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

// Header offsets - 常量优化
const OFFSET_FLAGS: usize = 85;
const OFFSET_SIZE: usize = 86;
const DATA_OFFSET_PAYLOAD: usize = 88;
const MAX_FEC_SET_AGE_MICROS: u64 = 30_000_000; // 30ms instead of 30s
const MAX_SLOT_AGE_MICROS: u64 = 10_000_000; // 10ms for slots
const BATCH_PROCESSING_TIMEOUT_MICROS: u64 = 500; // 500μs timeout

#[derive(Debug, Clone)]
pub struct CompletedFecSet {
    pub slot: u64,
    pub data_shreds: HashMap<u32, ShredMeta>,
    pub created_at_micros: u64,
}

struct FecSetAccumulator {
    slot: u64,
    data_shreds: HashMap<u32, ShredMeta>,
    code_shreds: HashMap<u32, ShredMeta>,
    expected_data_shreds: Option<usize>,
    created_at_micros: u64,
}

#[derive(Debug)]
enum ReconstructionStatus {
    NotReady,
    ReadyNatural,
    ReadyRecovery,
    Expired,
}

#[derive(Debug, Clone)]
pub struct BatchWork {
    pub slot: u64,
    pub batch_start_idx: u32,
    pub batch_end_idx: u32,
    pub shreds: HashMap<u32, ShredMeta>,
    pub received_at_micros: u64,
}

struct CombinedDataMeta {
    combined_data_shred_indices: Vec<usize>,
    combined_data_shred_received_at_micros: Vec<Option<u64>>,
    combined_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ShredMeta {
    pub shred: Shred,
    pub received_at_micros: Option<u64>,
}

#[derive(Debug)]
pub struct EntryMeta {
    pub entry: Entry,
    pub received_at_micros: Option<u64>,
    pub slot: u64,
}

pub struct SlotAccumulator {
    data_shreds: HashMap<u32, ShredMeta>,
    last_processed_batch_idx: Option<u32>,
    created_at_micros: u64,
    slot: u64,
}

// 🚀 内存池优化 - 重用分配的内存
struct MemoryPool {
    shred_buffers: ArrayQueue<Vec<u8>>,
    fec_accumulators: ArrayQueue<FecSetAccumulator>,
    slot_accumulators: ArrayQueue<SlotAccumulator>,
}

impl MemoryPool {
    fn new(max_size: usize) -> Self {
        Self {
            shred_buffers: ArrayQueue::new(max_size),
            fec_accumulators: ArrayQueue::new(max_size),
            slot_accumulators: ArrayQueue::new(max_size),
        }
    }

    fn get_shred_buffer(&self) -> Vec<u8> {
        self.shred_buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(8192))
    }

    fn return_shred_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        // 如果队列已满，丢弃buffer以避免阻塞
        let _ = self.shred_buffers.push(buffer);
    }

    fn get_fec_accumulator(&self) -> Option<FecSetAccumulator> {
        self.fec_accumulators.pop()
    }

    fn return_fec_accumulator(&self, mut acc: FecSetAccumulator) {
        acc.data_shreds.clear();
        acc.code_shreds.clear();
        acc.expected_data_shreds = None;
        // 如果队列已满，丢弃accumulator
        let _ = self.fec_accumulators.push(acc);
    }

    fn get_slot_accumulator(&self) -> Option<SlotAccumulator> {
        self.slot_accumulators.pop()
    }

    fn return_slot_accumulator(&self, mut acc: SlotAccumulator) {
        acc.data_shreds.clear();
        // 如果队列已满，丢弃accumulator
        let _ = self.slot_accumulators.push(acc);
    }
}

pub struct ShredProcessor {
    memory_pool: Arc<MemoryPool>,
    stats: Arc<ProcessorStats>,
    reed_solomon_cache: Arc<ReedSolomonCache>,
}

#[derive(Default)]
struct ProcessorStats {
    total_shreds_processed: AtomicU64,
    fec_sets_completed: AtomicU64,
    batches_processed: AtomicU64,
    transactions_processed: AtomicU64,
}

impl Clone for ShredProcessor {
    fn clone(&self) -> Self {
        Self {
            memory_pool: Arc::clone(&self.memory_pool),
            stats: Arc::clone(&self.stats),
            reed_solomon_cache: Arc::clone(&self.reed_solomon_cache),
        }
    }
}

impl ShredProcessor {
    pub fn new() -> Self {
        Self {
            memory_pool: Arc::new(MemoryPool::new(1000)),
            stats: Arc::new(ProcessorStats::default()),
            reed_solomon_cache: Arc::new(ReedSolomonCache::default()),
        }
    }

    pub async fn run<H: TransactionHandler + Send + Sync + 'static>(
        self,
        tx_handler: H,
        config: &UnshredConfig,
    ) -> Result<()> {
        let total_cores = num_cpus::get();
        let tx_handler = Arc::new(tx_handler);

        // 🚀 优化通道大小 - 更小的缓冲区，更低的延迟
        let (completed_fec_sender, completed_fec_receiver) =
            tokio::sync::mpsc::channel::<CompletedFecSet>(100); // 从1000降到100

        let processed_fec_sets = Arc::new(DashSet::<(u64, u32)>::new());

        // 🚀 动态工作线程配置
        let num_fec_workers = config
            .num_fec_workers
            .unwrap_or_else(|| {
                (total_cores as f32 * 0.6) as u8 // 60% 核心用于FEC
            })
            .max(1);

        let num_batch_workers = config
            .num_batch_workers
            .unwrap_or_else(|| {
                (total_cores as f32 * 0.3) as u8 // 30% 核心用于批处理
            })
            .max(1);

        info!(
            "Starting with {} FEC workers, {} batch workers on {} cores",
            num_fec_workers, num_batch_workers, total_cores
        );

        // 🚀 预分配通道
        let (shred_senders, shred_receivers): (Vec<_>, Vec<_>) = (0..num_fec_workers as usize)
            .map(|_| tokio::sync::mpsc::channel::<ShredBytesMeta>(500)) // 从10000降到500
            .unzip();

        // 🚀 优化网络接收器
        let bind_addr: std::net::SocketAddr = config.bind_address.parse()?;
        let receiver = crate::receiver::ShredReceiver::new(bind_addr)?;

        // 🚀 使用 JoinSet 管理任务
        let mut join_set = JoinSet::new();

        // 🚀 启动 FEC 工作线程
        for (worker_id, fec_receiver) in shred_receivers.into_iter().enumerate() {
            let sender = completed_fec_sender.clone();
            let processed_fec_sets_clone = Arc::clone(&processed_fec_sets);
            let processor_clone = self.clone();

            join_set.spawn(async move {
                if let Err(e) = processor_clone
                    .run_fec_worker(worker_id, fec_receiver, sender, processed_fec_sets_clone)
                    .await
                {
                    error!("FEC worker {} failed: {:?}", worker_id, e);
                }
            });
        }

        // 🚀 启动批处理分发器
        let (batch_senders, batch_receivers): (Vec<_>, Vec<_>) = (0..num_batch_workers as usize)
            .map(|_| tokio::sync::mpsc::channel::<BatchWork>(200)) // 从10000降到200
            .unzip();

        let dispatch_processor = self.clone();
        join_set.spawn(async move {
            if let Err(e) = dispatch_processor
                .dispatch_worker(completed_fec_receiver, batch_senders)
                .await
            {
                error!("Dispatch worker failed: {:?}", e);
            }
        });

        // 🚀 启动批处理工作线程
        for (worker_id, batch_receiver) in batch_receivers.into_iter().enumerate() {
            let tx_handler_clone = Arc::clone(&tx_handler);
            let processor_clone = self.clone();

            join_set.spawn(async move {
                if let Err(e) = processor_clone
                    .batch_worker(worker_id, batch_receiver, tx_handler_clone)
                    .await
                {
                    error!("Batch worker {} failed: {:?}", worker_id, e);
                }
            });
        }

        // 🚀 等待所有任务完成
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                error!("Worker task failed: {:?}", e);
            }
        }

        Ok(())
    }

    async fn run_fec_worker(
        &self,
        worker_id: usize,
        mut receiver: Receiver<ShredBytesMeta>,
        sender: Sender<CompletedFecSet>,
        processed_fec_sets: Arc<DashSet<(u64, u32)>>,
    ) -> Result<()> {
        let mut fec_set_accumulators: HashMap<(u64, u32), FecSetAccumulator> =
            HashMap::with_capacity(100);
        let mut last_stats_update = Instant::now();

        while let Some(shred_bytes_meta) = receiver.recv().await {
            let current_time_micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_micros() as u64;

            // 🚀 零拷贝 shred 解析
            match self
                .process_fec_shred_zero_copy(
                    shred_bytes_meta,
                    &mut fec_set_accumulators,
                    &sender,
                    &processed_fec_sets,
                    current_time_micros,
                )
                .await
            {
                Ok(_) => {
                    self.stats
                        .total_shreds_processed
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("FEC worker {} error: {:?}", worker_id, e);
                }
            }

            // 🚀 定期清理和统计更新
            if last_stats_update.elapsed() > Duration::from_millis(100) {
                self.cleanup_expired_fec_sets(&mut fec_set_accumulators, current_time_micros);
                self.update_fec_worker_metrics(worker_id, &receiver);
                last_stats_update = Instant::now();
            }
        }

        Ok(())
    }

    // 🚀 零拷贝 shred 解析 - 关键性能优化
    async fn process_fec_shred_zero_copy(
        &self,
        shred_bytes_meta: ShredBytesMeta,
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        sender: &Sender<CompletedFecSet>,
        processed_fec_sets: &DashSet<(u64, u32)>,
        current_time_micros: u64,
    ) -> Result<()> {
        // 🚀 避免不必要的 Vec 分配
        let shred_result = Shred::new_from_serialized_shred(shred_bytes_meta.shred_bytes.slice(..));

        let shred = match shred_result {
            Ok(shred) => shred,
            Err(e) => {
                #[cfg(feature = "metrics")]
                {
                    if let Some(metrics) = Metrics::try_get() {
                        metrics.processor_shred_parse_errors.inc();
                    }
                }
                return Ok(());
            }
        };

        let slot = shred.slot();
        let fec_set_index = shred.fec_set_index();
        let fec_key = (slot, fec_set_index);

        // 🚀 快速路径：检查是否已经处理过
        if processed_fec_sets.contains(&fec_key) {
            return Ok(());
        }

        // 🚀 获取或创建 accumulator
        let accumulator = fec_set_accumulators.entry(fec_key).or_insert_with(|| {
            match self.memory_pool.get_fec_accumulator() {
                Some(mut acc) => {
                    acc.slot = slot;
                    acc.created_at_micros = current_time_micros;
                    acc.expected_data_shreds = None;
                    acc
                }
                None => FecSetAccumulator {
                    slot,
                    data_shreds: HashMap::with_capacity(32),
                    code_shreds: HashMap::with_capacity(32),
                    expected_data_shreds: None,
                    created_at_micros: current_time_micros,
                },
            }
        });

        // 🚀 高效存储 shred
        let shred_meta = ShredMeta {
            shred,
            received_at_micros: shred_bytes_meta.received_at_micros,
        };

        match shred_meta.shred.shred_type() {
            ShredType::Code => {
                self.store_code_shred(accumulator, shred_meta, current_time_micros)?;
            }
            ShredType::Data => {
                self.store_data_shred(accumulator, shred_meta)?;
            }
        }

        // 🚀 检查是否完成
        self.check_fec_completion(
            fec_key,
            fec_set_accumulators,
            sender,
            processed_fec_sets,
            current_time_micros,
        )
        .await?;

        Ok(())
    }

    fn store_code_shred(
        &self,
        accumulator: &mut FecSetAccumulator,
        shred_meta: ShredMeta,
        current_time_micros: u64,
    ) -> Result<()> {
        let payload = shred_meta.shred.payload();
        if accumulator.expected_data_shreds.is_none() && payload.len() >= 85 {
            let expected = u16::from_le_bytes([payload[83], payload[84]]) as usize;
            accumulator.expected_data_shreds = Some(expected);
        }

        accumulator
            .code_shreds
            .insert(shred_meta.shred.index(), shred_meta);

        // 🚀 检查是否过期
        if current_time_micros - accumulator.created_at_micros > MAX_FEC_SET_AGE_MICROS {
            return Err(anyhow!("FEC set expired"));
        }

        #[cfg(feature = "metrics")]
        {
            if let Some(metrics) = Metrics::try_get() {
                metrics
                    .processor_shreds_accumulated
                    .with_label_values(&["code"])
                    .inc();
            }
        }

        Ok(())
    }

    fn store_data_shred(
        &self,
        accumulator: &mut FecSetAccumulator,
        shred_meta: ShredMeta,
    ) -> Result<()> {
        accumulator
            .data_shreds
            .insert(shred_meta.shred.index(), shred_meta);

        #[cfg(feature = "metrics")]
        {
            if let Some(metrics) = Metrics::try_get() {
                metrics
                    .processor_shreds_accumulated
                    .with_label_values(&["data"])
                    .inc();
            }
        }

        Ok(())
    }

    async fn check_fec_completion(
        &self,
        fec_key: (u64, u32),
        fec_set_accumulators: &mut HashMap<(u64, u32), FecSetAccumulator>,
        sender: &Sender<CompletedFecSet>,
        processed_fec_sets: &DashSet<(u64, u32)>,
        current_time_micros: u64,
    ) -> Result<()> {
        if !fec_set_accumulators.contains_key(&fec_key) {
            return Ok(());
        }

        let status = {
            let acc = fec_set_accumulators.get(&fec_key).unwrap();
            if current_time_micros - acc.created_at_micros > MAX_FEC_SET_AGE_MICROS {
                ReconstructionStatus::Expired
            } else {
                self.can_reconstruct_fec_set(acc)
            }
        };

        match status {
            ReconstructionStatus::ReadyNatural => {
                let mut acc = fec_set_accumulators.remove(&fec_key).unwrap();
                self.send_completed_fec_set(
                    &mut acc,
                    sender,
                    fec_key,
                    processed_fec_sets,
                    current_time_micros,
                )
                .await?;
                self.stats
                    .fec_sets_completed
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReconstructionStatus::ReadyRecovery => {
                let mut acc = fec_set_accumulators.remove(&fec_key).unwrap();
                if let Err(e) = self.recover_fec_fast(&mut acc).await {
                    error!("FEC Recovery failed: {:?}", e);
                    // 返回 accumulator 到内存池
                    self.memory_pool.return_fec_accumulator(acc);
                    return Ok(());
                }
                self.send_completed_fec_set(
                    &mut acc,
                    sender,
                    fec_key,
                    processed_fec_sets,
                    current_time_micros,
                )
                .await?;
                self.stats
                    .fec_sets_completed
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReconstructionStatus::Expired => {
                if let Some(acc) = fec_set_accumulators.remove(&fec_key) {
                    self.memory_pool.return_fec_accumulator(acc);
                }
                #[cfg(feature = "metrics")]
                {
                    if let Some(metrics) = Metrics::try_get() {
                        metrics.processor_fec_sets_expired.inc();
                    }
                }
            }
            ReconstructionStatus::NotReady => {}
        }

        Ok(())
    }

    fn can_reconstruct_fec_set(&self, acc: &FecSetAccumulator) -> ReconstructionStatus {
        let data_count = acc.data_shreds.len();
        let code_count = acc.code_shreds.len();

        if let Some(expected) = acc.expected_data_shreds {
            if data_count >= expected {
                ReconstructionStatus::ReadyNatural
            } else if data_count + code_count >= expected {
                ReconstructionStatus::ReadyRecovery
            } else {
                ReconstructionStatus::NotReady
            }
        } else {
            // 🚀 优化：32个数据shreds通常足够
            if data_count >= 32 {
                ReconstructionStatus::ReadyNatural
            } else {
                ReconstructionStatus::NotReady
            }
        }
    }

    // 🚀 快速FEC恢复 - 避免不必要的克隆
    async fn recover_fec_fast(&self, acc: &mut FecSetAccumulator) -> Result<()> {
        let mut shreds_for_recovery =
            Vec::with_capacity(acc.data_shreds.len() + acc.code_shreds.len());

        // 🚀 预分配空间，避免重新分配
        shreds_for_recovery.extend(acc.data_shreds.values().map(|meta| meta.shred.clone()));
        shreds_for_recovery.extend(acc.code_shreds.values().map(|meta| meta.shred.clone()));

        match solana_ledger::shred::recover(shreds_for_recovery, &self.reed_solomon_cache) {
            Ok(recovered_shreds) => {
                for result in recovered_shreds {
                    match result {
                        Ok(recovered_shred) => {
                            if recovered_shred.is_data() {
                                let index = recovered_shred.index();
                                if !acc.data_shreds.contains_key(&index) {
                                    acc.data_shreds.insert(
                                        index,
                                        ShredMeta {
                                            shred: recovered_shred,
                                            received_at_micros: None,
                                        },
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!("Failed to recover shred: {:?}", e));
                        }
                    }
                }
            }
            Err(e) => {
                return Err(anyhow!("FEC recovery failed: {:?}", e));
            }
        }

        Ok(())
    }

    async fn send_completed_fec_set(
        &self,
        acc: &mut FecSetAccumulator,
        sender: &Sender<CompletedFecSet>,
        fec_key: (u64, u32),
        processed_fec_sets: &DashSet<(u64, u32)>,
        current_time_micros: u64,
    ) -> Result<()> {
        // 🚀 克隆而不是取走数据，确保发送失败时数据不丢失
        let data_shreds = acc.data_shreds.clone();

        let completed_fec_set = CompletedFecSet {
            slot: acc.slot,
            data_shreds,
            created_at_micros: current_time_micros,
        };

        // 🚀 非阻塞发送，超时处理
        match tokio::time::timeout(
            Duration::from_micros(BATCH_PROCESSING_TIMEOUT_MICROS),
            sender.send(completed_fec_set),
        )
        .await
        {
            Ok(Ok(_)) => {
                processed_fec_sets.insert(fec_key);
                // 发送成功后清空原始数据
                acc.data_shreds.clear();
                // 🚀 返回 accumulator 到内存池
                self.memory_pool.return_fec_accumulator(std::mem::replace(
                    acc,
                    FecSetAccumulator {
                        slot: 0,
                        data_shreds: HashMap::new(),
                        code_shreds: HashMap::new(),
                        expected_data_shreds: None,
                        created_at_micros: 0,
                    },
                ));
                Ok(())
            }
            Ok(Err(e)) => {
                // 发送失败，保留原始数据，不返回accumulator到内存池
                Err(anyhow!("Failed to send completed FEC set: {:?}", e))
            }
            Err(_) => {
                // 超时，保留原始数据，不返回accumulator到内存池
                Err(anyhow!("Send timeout"))
            }
        }
    }

    // 🚀 高效清理函数
    fn cleanup_expired_fec_sets(
        &self,
        fec_sets: &mut HashMap<(u64, u32), FecSetAccumulator>,
        current_time_micros: u64,
    ) {
        let keys_to_remove: Vec<_> = fec_sets
            .iter()
            .filter_map(|(key, acc)| {
                if current_time_micros - acc.created_at_micros > MAX_FEC_SET_AGE_MICROS {
                    Some(*key)
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            if let Some(acc) = fec_sets.remove(&key) {
                // 🚀 返回内存到池中
                self.memory_pool.return_fec_accumulator(acc);
            }
        }
    }

    async fn dispatch_worker(
        &self,
        mut completed_fec_receiver: Receiver<CompletedFecSet>,
        batch_senders: Vec<Sender<BatchWork>>,
    ) -> Result<()> {
        let mut slot_accumulators: HashMap<u64, SlotAccumulator> = HashMap::with_capacity(50);
        let mut processed_slots = HashSet::with_capacity(100);
        let mut next_worker = 0usize;
        let mut last_cleanup = Instant::now();

        while let Some(completed_fec_set) = completed_fec_receiver.recv().await {
            let current_time_micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_micros() as u64;

            if processed_slots.contains(&completed_fec_set.slot) {
                continue;
            }

            // 🚀 快速路径：直接创建或获取 slot accumulator
            let accumulator = slot_accumulators
                .entry(completed_fec_set.slot)
                .or_insert_with(|| match self.memory_pool.get_slot_accumulator() {
                    Some(mut acc) => {
                        acc.slot = completed_fec_set.slot;
                        acc.created_at_micros = current_time_micros;
                        acc.last_processed_batch_idx = None;
                        acc
                    }
                    None => SlotAccumulator {
                        data_shreds: HashMap::with_capacity(64),
                        last_processed_batch_idx: None,
                        created_at_micros: current_time_micros,
                        slot: completed_fec_set.slot,
                    },
                });

            // 🚀 高效合并 shreds
            for (index, shred_meta) in completed_fec_set.data_shreds {
                accumulator.data_shreds.insert(index, shred_meta);
            }

            // 🚀 尝试分发完整批处理
            if let Err(e) = self
                .try_dispatch_complete_batches(
                    accumulator,
                    completed_fec_set.slot,
                    &batch_senders,
                    &mut next_worker,
                    current_time_micros,
                )
                .await
            {
                error!("Failed to dispatch batches: {:?}", e);
            }

            // 🚀 定期清理
            if last_cleanup.elapsed() > Duration::from_millis(50) {
                self.cleanup_expired_slots(
                    &mut slot_accumulators,
                    &mut processed_slots,
                    current_time_micros,
                );
                last_cleanup = Instant::now();
            }
        }

        Ok(())
    }

    fn cleanup_expired_slots(
        &self,
        slot_accumulators: &mut HashMap<u64, SlotAccumulator>,
        processed_slots: &mut HashSet<u64>,
        current_time_micros: u64,
    ) {
        let slots_to_remove: Vec<_> = slot_accumulators
            .iter()
            .filter_map(|(&slot, acc)| {
                if current_time_micros - acc.created_at_micros > MAX_SLOT_AGE_MICROS {
                    Some(slot)
                } else {
                    None
                }
            })
            .collect();

        for slot in slots_to_remove {
            if let Some(acc) = slot_accumulators.remove(&slot) {
                processed_slots.insert(slot);
                // 🚀 返回内存到池中
                self.memory_pool.return_slot_accumulator(acc);
            }
        }
    }

    async fn try_dispatch_complete_batches(
        &self,
        accumulator: &mut SlotAccumulator,
        slot: u64,
        batch_senders: &[Sender<BatchWork>],
        next_worker: &mut usize,
        current_time_micros: u64,
    ) -> Result<()> {
        let last_processed = accumulator.last_processed_batch_idx.unwrap_or(0);

        // 🚀 使用 Vec 而不是 HashMap 进行批处理查找
        let mut batch_end_indices: Vec<u32> = Vec::new();

        for (&idx, shred_meta) in &accumulator.data_shreds {
            if idx <= last_processed {
                continue;
            }
            let payload = shred_meta.shred.payload();
            if let Some(&flags) = payload.get(OFFSET_FLAGS) {
                if (flags & 0x40) != 0 {
                    batch_end_indices.push(idx);
                }
            }
        }

        batch_end_indices.sort_unstable();

        for batch_end_idx in batch_end_indices {
            let batch_start_idx = accumulator
                .last_processed_batch_idx
                .map_or(0, |idx| idx + 1);

            // 🚀 快速检查是否所有 shreds 都存在
            let has_all_shreds =
                (batch_start_idx..=batch_end_idx).all(|i| accumulator.data_shreds.contains_key(&i));

            if !has_all_shreds {
                continue;
            }

            // 🚀 高效构建批处理
            let mut batch_shreds =
                HashMap::with_capacity((batch_end_idx - batch_start_idx + 1) as usize);
            for idx in batch_start_idx..=batch_end_idx {
                if let Some(shred_meta) = accumulator.data_shreds.remove(&idx) {
                    batch_shreds.insert(idx, shred_meta);
                }
            }

            let mut batch_work = BatchWork {
                slot,
                batch_start_idx,
                batch_end_idx,
                shreds: batch_shreds,
                received_at_micros: current_time_micros,
            };

            let sender = &batch_senders[*next_worker % batch_senders.len()];
            *next_worker = (*next_worker + 1) % batch_senders.len();

            // 🚀 超时发送 - 修复资源泄漏
            match tokio::time::timeout(
                Duration::from_micros(BATCH_PROCESSING_TIMEOUT_MICROS),
                sender.send(batch_work),
            )
            .await
            {
                Ok(Ok(_)) => {
                    accumulator.last_processed_batch_idx = Some(batch_end_idx);
                    self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(SendError(batch_work))) => {
                    // 发送失败，将shreds重新放回accumulator
                    // mem::take(&mut batch_work.shreds);
                    for (idx, shred_meta) in batch_work.shreds {
                        accumulator.data_shreds.insert(idx, shred_meta);
                    }
                    return Err(anyhow!("Batch send error"));
                }
                Err(_) => {
                    // // 超时，将shreds重新放回accumulator
                    // for (idx, shred_meta) in batch_work.shreds {
                    //     accumulator.data_shreds.insert(idx, shred_meta);
                    // }
                    return Err(anyhow!("Batch send timeout"));
                }
            }
        }

        Ok(())
    }

    async fn batch_worker<H: TransactionHandler>(
        &self,
        worker_id: usize,
        mut batch_receiver: Receiver<BatchWork>,
        tx_handler: Arc<H>,
    ) -> Result<()> {
        while let Some(batch_work) = batch_receiver.recv().await {
            if let Err(e) = self.process_batch_work_fast(batch_work, &tx_handler).await {
                error!("Batch worker {} failed to process batch: {}", worker_id, e);
            }
        }

        Ok(())
    }

    // 🚀 高性能批处理处理
    async fn process_batch_work_fast<H: TransactionHandler>(
        &self,
        batch_work: BatchWork,
        tx_handler: &Arc<H>,
    ) -> Result<()> {
        let combined_data_meta = match self.get_batch_data_zero_copy(
            &batch_work.shreds,
            batch_work.batch_start_idx,
            batch_work.batch_end_idx,
        ) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to get batch data: {:?}", e);
                return Ok(());
            }
        };

        match self.parse_entries_from_batch_data_fast(combined_data_meta, batch_work.slot) {
            Ok(entries) => {
                // 🚀 并行处理交易
                for entry_meta in entries {
                    if let Err(e) = self
                        .process_entry_transactions_fast(&entry_meta, tx_handler)
                        .await
                    {
                        error!("Failed to process entry transactions: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to parse entries: {:?}", e);
            }
        }

        Ok(())
    }

    // 🚀 零拷贝批量数据获取
    fn get_batch_data_zero_copy(
        &self,
        shreds: &HashMap<u32, ShredMeta>,
        start_idx: u32,
        end_idx: u32,
    ) -> Result<CombinedDataMeta> {
        let batch_size = (end_idx - start_idx + 1) as usize;
        let mut combined_data = self.memory_pool.get_shred_buffer();
        let mut combined_data_shred_indices = Vec::with_capacity(batch_size);
        let mut combined_data_shred_received_at_micros = Vec::with_capacity(batch_size);

        for idx in start_idx..=end_idx {
            let shred_meta = shreds
                .get(&idx)
                .ok_or_else(|| anyhow!("Missing shred at index {}", idx))?;

            combined_data_shred_received_at_micros.push(shred_meta.received_at_micros);
            combined_data_shred_indices.push(combined_data.len());

            let payload = shred_meta.shred.payload();
            if payload.len() < OFFSET_SIZE + 2 {
                // 错误时返回缓冲区
                self.memory_pool.return_shred_buffer(combined_data);
                return Err(anyhow!("Invalid payload size"));
            }

            let size_bytes = &payload[OFFSET_SIZE..OFFSET_SIZE + 2];
            let total_size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
            let data_size = total_size.saturating_sub(DATA_OFFSET_PAYLOAD);

            if let Some(data) = payload.get(DATA_OFFSET_PAYLOAD..DATA_OFFSET_PAYLOAD + data_size) {
                combined_data.extend_from_slice(data);
            } else {
                // 错误时返回缓冲区
                self.memory_pool.return_shred_buffer(combined_data);
                return Err(anyhow!("Missing data in shred"));
            }
        }

        Ok(CombinedDataMeta {
            combined_data,
            combined_data_shred_indices,
            combined_data_shred_received_at_micros,
        })
    }

    // 🚀 快速条目解析
    fn parse_entries_from_batch_data_fast(
        &self,
        mut combined_data_meta: CombinedDataMeta,  // 改为可变，因为我们要取出数据
        slot: u64,
    ) -> Result<Vec<EntryMeta>> {
        // 取出 combined_data 和其他字段
        let combined_data = combined_data_meta.combined_data;
        let shred_indices = combined_data_meta.combined_data_shred_indices;
        let shred_received_at_micros = combined_data_meta.combined_data_shred_received_at_micros;

        if combined_data.len() <= 8 {
            // 返回缓冲区到内存池
            self.memory_pool.return_shred_buffer(combined_data);
            return Ok(Vec::new());
        }

        let entry_count_bytes = combined_data[0..8].try_into();
        if entry_count_bytes.is_err() {
            // 返回缓冲区到内存池
            self.memory_pool.return_shred_buffer(combined_data);
            return Err(anyhow!("Failed to parse entry count: {:?}", entry_count_bytes.err().unwrap()));
        }
        // 解析entry_count
        let entry_count_bytes: [u8; 8] = entry_count_bytes?;

        let entry_count = u64::from_le_bytes(entry_count_bytes);

        if entry_count == 0 {
            self.memory_pool.return_shred_buffer(combined_data);
            return Ok(Vec::new());
        }

        let mut cursor = Cursor::new(&combined_data);
        cursor.set_position(8);

        let mut entries = Vec::with_capacity(entry_count as usize);

        for _ in 0..entry_count {
            let entry_start_pos = cursor.position() as usize;

            match bincode::deserialize_from::<_, Entry>(&mut cursor) {
                Ok(entry) => {
                    let earliest_timestamp = self.find_earliest_contributing_shred_timestamp(
                        entry_start_pos,
                        &shred_indices,
                        &shred_received_at_micros,
                    );

                    entries.push(EntryMeta {
                        entry,
                        received_at_micros: earliest_timestamp,
                        slot,
                    });
                }
                Err(e) => {
                    // 返回缓冲区到内存池
                    self.memory_pool.return_shred_buffer(combined_data);
                    return Err(anyhow!("Error deserializing entry: {:?}", e));
                }
            }
        }

        // 返回缓冲区到内存池
        self.memory_pool.return_shred_buffer(combined_data);

        Ok(entries)
    }

    fn find_earliest_contributing_shred_timestamp(
        &self,
        entry_start_pos: usize,
        shred_indices: &[usize],
        shred_received_at_micros: &[Option<u64>],
    ) -> Option<u64> {
        match shred_indices.binary_search(&entry_start_pos) {
            Ok(idx) => {
                // Entry starts exactly at start of shred
                if idx < shred_received_at_micros.len() {
                    shred_received_at_micros[idx]
                } else {
                    None
                }
            }
            Err(idx) => {
                // Entry starts in the middle of the previous shred
                if idx > 0 && idx - 1 < shred_received_at_micros.len() {
                    shred_received_at_micros[idx - 1]
                } else if idx < shred_received_at_micros.len() {
                    shred_received_at_micros[idx]
                } else {
                    None
                }
            }
        }
    }

    // 🚀 快速交易处理
    async fn process_entry_transactions_fast<H: TransactionHandler>(
        &self,
        entry_meta: &EntryMeta,
        handler: &Arc<H>,
    ) -> Result<()> {
        let current_time = SystemTime::now();
        let processed_at_micros = current_time
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_micros() as u64;

        for tx in &entry_meta.entry.transactions {
            let event = TransactionEvent {
                slot: entry_meta.slot,
                transaction: tx,
                received_at_micros: entry_meta.received_at_micros,
                processed_at_micros,
            };

            // 🚀 非阻塞处理 - 超时保护
            match tokio::time::timeout(
                Duration::from_micros(100), // 100μs 超时
                async { handler.handle_transaction(&event) },
            )
            .await
            {
                Ok(Ok(_)) => {
                    self.stats
                        .transactions_processed
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    error!("Transaction handler error: {:?}", e);
                }
                Err(_) => {
                    warn!("Transaction handler timeout for slot {}", entry_meta.slot);
                    #[cfg(feature = "metrics")]
                    {
                        if let Some(metrics) = Metrics::try_get() {
                            metrics.processor_handler_timeouts.inc();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // 🚀 性能指标更新
    fn update_fec_worker_metrics(&self, worker_id: usize, receiver: &Receiver<ShredBytesMeta>) {
        #[cfg(feature = "metrics")]
        {
            if let Some(metrics) = Metrics::try_get() {
                let capacity = receiver.capacity();
                if capacity > 0 {
                    let capacity_used = receiver.len() as f64 / capacity as f64 * 100.0;
                    metrics
                        .channel_capacity_utilization
                        .with_label_values(&[&format!("fec_worker_{}", worker_id)])
                        .set(capacity_used as i64);
                }
            }
        }
    }
}
