/**
 * twitter 的snowflake算法
 *  https://github.com/twitter/snowflake/blob/master/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
 * 协议格式(64位)：
 * 0     - 0000000000 0000000000 0000000000 0000000000 0 - 00000          - 00000       - 000000000000
 * 符号位 - 41位时间戳(毫秒级,时间为与EPOCH时间的差值)        - 5位数据中心标识 - 5位机器标识   - 12位序列号
 * 并发支持最高同一时间戳内产生4096个ID
 * 优点：
 *  基本按照时间递增容易排序
 * 注意：
 *  对系统时间敏感，修改系统时间将导致无法生成新ID
 */
public class SnowFlake {

    // 开始时间戳
    private final static long EPOCH = 1514736000000L;

    private final static long WORKER_ID_BITS = 5L;
    private final static long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);

    private final static long DATACENTER_ID_BITS = 5L;
    private final static long MAX_DATACENTER_ID = -1L ^ (-1L << DATACENTER_ID_BITS);

    private final static long SEQUENCE_BITS = 12L;

    private final static long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private final static long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private final static long TIMESTAMP_LEFT_SHIFT = DATACENTER_ID_SHIFT + DATACENTER_ID_BITS;

    private final static long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

    private long lastTimestamp = -1L;

    private final long workerId;
    private final long datacenterId;
    private long sequence = 0L; //序列号

    public SnowFlake(long workerId, long datacenterId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(String.format("Worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
        }
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("Datacenter Id can't be greater than %d or less than 0", MAX_DATACENTER_ID));
        }

        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    public synchronized long nextId() {

        long timestamp = getTimestamp();
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards. Rejecting requests until %d.  Refusing to generate id for %d milliseconds",
                    lastTimestamp, lastTimestamp - timestamp));
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0L) {
                timestamp = tilNextMillis();
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - EPOCH) << TIMESTAMP_LEFT_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long tilNextMillis() {
        long timestamp = getTimestamp();
        while (timestamp <= lastTimestamp) {
            timestamp = getTimestamp();
        }
        return timestamp;
    }

    private long getTimestamp() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

    public long getDatacenterId() {
        return datacenterId;
    }

    @Override
    public String toString() {
        return String.format("timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d, datacenterId %d",
                TIMESTAMP_LEFT_SHIFT, DATACENTER_ID_BITS, WORKER_ID_BITS, SEQUENCE_BITS, workerId, datacenterId);
    }

}