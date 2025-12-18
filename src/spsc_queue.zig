const std = @import("std");

/// Lock-free Single Producer Single Consumer (SPSC) ring queue
///
/// It works with exactly one producer thread and one consumer thread.
///
/// - Producer: Main thread adding WAL events to batch
/// - Consumer: Flush thread publishing batches to NATS
///
/// It stores values, not pointers. The rung buffer
///
/// Memory ordering:
/// - Producer uses .release on write_index to ensure data is visible before index
/// - Consumer uses .acquire on write_index to ensure it sees the data
/// - Consumer uses .release on read_index so producer sees space is available
///
/// Properties:
/// - Single Producer, Single Consumer with fixed capacity a power of 2
/// - uses a ring buffer and computes indices via bitmasking
/// - leaves one slot empty to distinguish full vs empty
/// - No locks, no syscalls, just atomic operations
/// - Cache alignment to avoid false sharing
pub fn SPSCQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        buffer: []T,
        capacity: usize,
        mask: usize, // capacity - 1, for fast modulo via bitmasking

        // Separate cache lines to avoid false sharing between producer/consumer
        // On x86-64, cache lines are 64 bytes
        write_index: std.atomic.Value(usize) align(64),
        read_index: std.atomic.Value(usize) align(64),

        allocator: std.mem.Allocator,

        /// Initialize a new SPSC queue with given capacity (must be power of 2)
        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            // Ensure capacity is power of 2 for fast modulo
            if (capacity == 0 or (capacity & (capacity - 1)) != 0) {
                return error.CapacityMustBePowerOfTwo;
            }

            const buffer = try allocator.alloc(T, capacity);

            return Self{
                .buffer = buffer,
                .capacity = capacity,
                .mask = capacity - 1,
                .write_index = std.atomic.Value(usize).init(0),
                .read_index = std.atomic.Value(usize).init(0),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        /// Push an item to the queue (producer only)
        /// Returns error.QueueFull if queue is at capacity
        ///
        /// Memory ordering:
        /// - acquire on read_index: See consumer's progress
        /// - release on write_index: Ensure data is visible before index update
        pub fn push(self: *Self, item: T) error{QueueFull}!void {
            const current_write = self.write_index.load(.monotonic);

            // We leave one slot empty to distinguish full from empty
            const next_write = (current_write + 1) & self.mask;
            const current_read = self.read_index.load(.acquire);
            // Check if queue is full
            if (next_write == current_read) {
                return error.QueueFull;
            }

            // Write data to buffer
            self.buffer[current_write] = item;

            // with `.release`, all ops are completed before this store
            self.write_index.store(next_write, .release);
        }

        /// Pop an item from the queue (consumer only)
        /// Returns null if queue is empty
        ///
        /// Memory ordering:
        /// - acquire on write_index: See producer's data
        /// - release on read_index: Make space visible to producer
        pub fn pop(self: *Self) ?T {
            const current_read = self.read_index.load(.monotonic);
            const current_write = self.write_index.load(.acquire);

            // Check if queue is empty
            if (current_read == current_write) {
                return null;
            }

            // Read data from buffer
            const item = self.buffer[current_read];

            const next_read = (current_read + 1) & self.mask;
            // with `.release`, all ops are completed before this store
            self.read_index.store(next_read, .release);

            return item;
        }

        /// Check if queue is empty (can be called by either thread)
        pub fn isEmpty(self: *Self) bool {
            const current_read = self.read_index.load(.monotonic);
            const current_write = self.write_index.load(.acquire);
            return current_read == current_write;
        }

        /// Get current number of items in queue (approximate, for monitoring only)
        /// Due to concurrent access, this is a snapshot and may be stale immediately
        pub fn len(self: *Self) usize {
            const current_read = self.read_index.load(.monotonic);
            const current_write = self.write_index.load(.monotonic);

            // Handle wrap-around
            if (current_write >= current_read) {
                return current_write - current_read;
            } else {
                return self.capacity - current_read + current_write;
            }
        }
    };
}

// Tests
test "SPSC queue basic" {
    const testing = std.testing;
    var queue = try SPSCQueue(u32).init(testing.allocator, 8);
    defer queue.deinit();

    try testing.expect(queue.isEmpty());

    // Push some items
    try queue.push(1);
    try queue.push(2);
    try queue.push(3);

    try testing.expect(!queue.isEmpty());
    try testing.expectEqual(@as(usize, 3), queue.len());

    // Pop items in order
    try testing.expectEqual(@as(u32, 1), queue.pop().?);
    try testing.expectEqual(@as(u32, 2), queue.pop().?);
    try testing.expectEqual(@as(u32, 3), queue.pop().?);

    try testing.expect(queue.isEmpty());
    try testing.expectEqual(@as(?u32, null), queue.pop());
}

test "SPSC queue full" {
    const testing = std.testing;
    var queue = try SPSCQueue(u32).init(testing.allocator, 4);
    defer queue.deinit();

    // Fill queue (capacity - 1 items to distinguish full from empty)
    try queue.push(1);
    try queue.push(2);
    try queue.push(3);

    // Next push should fail
    try testing.expectError(error.QueueFull, queue.push(4));

    // Pop one item
    _ = queue.pop();

    // Now we can push again
    try queue.push(4);
}

test "SPSC queue push-pop" {
    const testing = std.testing;
    var queue = try SPSCQueue(u32).init(testing.allocator, 4);
    defer queue.deinit();

    // Fill and drain multiple times to test wrap-around
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try queue.push(i);
        try testing.expectEqual(i, queue.pop().?);
    }

    try testing.expect(queue.isEmpty());
}
