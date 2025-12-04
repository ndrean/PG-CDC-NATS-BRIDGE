const std = @import("std");
const SPSCQueue = @import("spsc_queue.zig").SPSCQueue;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== SPSC Queue Concurrent Stress Test ===\n\n", .{});

    // Run multiple test scenarios
    try testBasicConcurrency(allocator);
    try testHighThroughput(allocator);
    try testProducerFasterThanConsumer(allocator);
    try testConsumerFasterThanProducer(allocator);
    try testBurstLoad(allocator);

    std.debug.print("\n✅ All concurrent tests passed!\n\n", .{});
}

const Event = struct { table_id: u32, lsn: u64 };

fn producer_thread(queue_ptr: *SPSCQueue(Event)) !void {
    const queue = queue_ptr;
    var evt_counter: u32 = 0;

    while (evt_counter < 1000) : (evt_counter += 1) {
        const event = Event{ .table_id = @as(u32, evt_counter % 10), .lsn = evt_counter };
        while (true) {
            if (queue.push(event)) |_| {
                break; // success
            } else |err| {
                if (err == error.QueueFull) {
                    // Queue full, yield and retry
                    try std.Thread.yield();
                } else {
                    // Unexpected error
                    @panic("Unexpected error in producer_thread");
                }
            }
        }
    }
    std.Thread.sleep(1);
}

fn consumer_thread(allocator: std.mem.Allocator, queue_ptr: *SPSCQueue(Event)) !void {
    const queue = queue_ptr;
    var consumed_count: usize = 0;

    var batch = std.ArrayList(Event){};
    defer batch.deinit(allocator);
    const BATCH_SIZE = 200;

    while (consumed_count < 1000) {
        // Clear batch for next iteration
        batch.clearRetainingCapacity();

        // Drain events from queue into batch
        while (batch.items.len < BATCH_SIZE) {
            if (queue.pop()) |e| {
                try batch.append(allocator, e);
            } else {
                break; // Queue is empty
            }
        }

        if (batch.items.len > 0) {
            // Process batch (simulate NATS publishing)
            consumed_count += batch.items.len;

            // Verify we haven't over-consumed
            if (consumed_count > 1000) {
                consumed_count = 1000;
                break;
            }
        } else {
            // Queue empty, yield to wait for producer
            try std.Thread.yield();
        }
    }
}

test "SPSC" {
    const testing = std.testing.allocator;
    const queue_capacity: usize = 32;
    var queue = try SPSCQueue(Event).init(testing, queue_capacity);
    defer queue.deinit();

    const producer = try std.Thread.spawn(.{}, producer_thread, .{&queue});
    const consumer = try std.Thread.spawn(.{}, consumer_thread, .{ std.testing.allocator, &queue });
    producer.join();
    consumer.join();
}

/// Basic concurrent test: Producer and consumer running simultaneously
fn testBasicConcurrency(allocator: std.mem.Allocator) !void {
    std.debug.print("Test 1: Basic Concurrency (100k items)\n", .{});

    const num_items = 100_000;
    var queue = try SPSCQueue(u64).init(allocator, 1024);
    defer queue.deinit();

    const Context = struct {
        queue: *SPSCQueue(u64),
        num_items: u64,
        stop: std.atomic.Value(bool),
    };

    var ctx = Context{
        .queue = &queue,
        .num_items = num_items,
        .stop = std.atomic.Value(bool).init(false),
    };

    // Producer thread
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var i: u64 = 0;
            while (i < context.num_items) {
                context.queue.push(i) catch {
                    // Queue full, yield and retry
                    std.atomic.spinLoopHint();
                    continue;
                };
                i += 1;
            }
        }
    }.run, .{&ctx});

    // Consumer thread
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var expected: u64 = 0;
            while (expected < context.num_items) {
                if (context.queue.pop()) |value| {
                    if (value != expected) {
                        std.debug.panic("❌ Order violation! Expected {d}, got {d}\n", .{ expected, value });
                    }
                    expected += 1;
                } else {
                    // Queue empty, yield briefly
                    std.atomic.spinLoopHint();
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    std.debug.print("  ✓ Produced and consumed {d} items in order\n", .{num_items});
}

/// High throughput test: Measure operations per second
fn testHighThroughput(allocator: std.mem.Allocator) !void {
    std.debug.print("\nTest 2: High Throughput (1M items)\n", .{});

    const num_items = 1_000_000;
    var queue = try SPSCQueue(u64).init(allocator, 2048);
    defer queue.deinit();

    const Context = struct {
        queue: *SPSCQueue(u64),
        num_items: u64,
        producer_done: std.atomic.Value(bool),
    };

    var ctx = Context{
        .queue = &queue,
        .num_items = num_items,
        .producer_done = std.atomic.Value(bool).init(false),
    };

    const start_time = std.time.milliTimestamp();

    // Producer thread
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var i: u64 = 0;
            while (i < context.num_items) {
                context.queue.push(i) catch {
                    std.atomic.spinLoopHint();
                    continue;
                };
                i += 1;
            }
            context.producer_done.store(true, .seq_cst);
        }
    }.run, .{&ctx});

    // Consumer thread
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var count: u64 = 0;
            while (count < context.num_items) {
                if (context.queue.pop()) |_| {
                    count += 1;
                } else {
                    std.atomic.spinLoopHint();
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    const end_time = std.time.milliTimestamp();
    const elapsed_ms = end_time - start_time;
    const ops_per_sec = @as(f64, @floatFromInt(num_items * 1000)) / @as(f64, @floatFromInt(elapsed_ms));

    std.debug.print("  ✓ {d} items in {d}ms ({d:.0} ops/sec)\n", .{ num_items, elapsed_ms, ops_per_sec });
}

/// Test producer faster than consumer (queue pressure)
fn testProducerFasterThanConsumer(allocator: std.mem.Allocator) !void {
    std.debug.print("\nTest 3: Producer Faster Than Consumer (backpressure)\n", .{});

    const num_items = 50_000;
    var queue = try SPSCQueue(u64).init(allocator, 512); // Smaller queue
    defer queue.deinit();

    const Context = struct {
        queue: *SPSCQueue(u64),
        num_items: u64,
        full_count: std.atomic.Value(u64),
        producer_done: std.atomic.Value(bool),
    };

    var ctx = Context{
        .queue = &queue,
        .num_items = num_items,
        .full_count = std.atomic.Value(u64).init(0),
        .producer_done = std.atomic.Value(bool).init(false),
    };

    // Fast producer
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var i: u64 = 0;
            while (i < context.num_items) {
                context.queue.push(i) catch {
                    _ = context.full_count.fetchAdd(1, .monotonic);
                    std.atomic.spinLoopHint();
                    continue;
                };
                i += 1;
            }
            context.producer_done.store(true, .seq_cst);
        }
    }.run, .{&ctx});

    // Slow consumer (simulates slower NATS publishing)
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var count: u64 = 0;
            while (count < context.num_items) {
                if (context.queue.pop()) |_| {
                    count += 1;
                    // Simulate slow processing
                    std.Thread.sleep(10); // 10ns sleep
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    const full_count = ctx.full_count.load(.seq_cst);
    std.debug.print("  ✓ Handled backpressure: queue full {d} times\n", .{full_count});
}

/// Test consumer faster than producer (queue often empty)
fn testConsumerFasterThanProducer(allocator: std.mem.Allocator) !void {
    std.debug.print("\nTest 4: Consumer Faster Than Producer (starvation)\n", .{});

    const num_items = 50_000;
    var queue = try SPSCQueue(u64).init(allocator, 1024);
    defer queue.deinit();

    const Context = struct {
        queue: *SPSCQueue(u64),
        num_items: u64,
        empty_count: std.atomic.Value(u64),
        producer_done: std.atomic.Value(bool),
    };

    var ctx = Context{
        .queue = &queue,
        .num_items = num_items,
        .empty_count = std.atomic.Value(u64).init(0),
        .producer_done = std.atomic.Value(bool).init(false),
    };

    // Slow producer
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var i: u64 = 0;
            while (i < context.num_items) {
                // Simulate slow WAL events
                std.Thread.sleep(10); // 10ns
                context.queue.push(i) catch unreachable; // Should never be full
                i += 1;
            }
            context.producer_done.store(true, .seq_cst);
        }
    }.run, .{&ctx});

    // Fast consumer
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var count: u64 = 0;
            while (count < context.num_items) {
                if (context.queue.pop()) |_| {
                    count += 1;
                } else {
                    _ = context.empty_count.fetchAdd(1, .monotonic);
                    std.atomic.spinLoopHint();
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    const empty_count = ctx.empty_count.load(.seq_cst);
    std.debug.print("  ✓ Consumer handled starvation: queue empty {d} times\n", .{empty_count});
}

/// Test burst load (alternating fast/slow periods)
fn testBurstLoad(allocator: std.mem.Allocator) !void {
    std.debug.print("\nTest 5: Burst Load (alternating fast/slow)\n", .{});

    const num_bursts = 100;
    const items_per_burst = 1000;
    const total_items = num_bursts * items_per_burst;

    var queue = try SPSCQueue(u64).init(allocator, 2048);
    defer queue.deinit();

    const Context = struct {
        queue: *SPSCQueue(u64),
        num_bursts: u64,
        items_per_burst: u64,
        producer_done: std.atomic.Value(bool),
    };

    var ctx = Context{
        .queue = &queue,
        .num_bursts = num_bursts,
        .items_per_burst = items_per_burst,
        .producer_done = std.atomic.Value(bool).init(false),
    };

    // Bursty producer
    const producer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            var item: u64 = 0;
            var burst: u64 = 0;

            while (burst < context.num_bursts) {
                // Fast burst
                var i: u64 = 0;
                while (i < context.items_per_burst) : (i += 1) {
                    while (true) {
                        context.queue.push(item) catch {
                            std.atomic.spinLoopHint();
                            continue;
                        };
                        break;
                    }
                    item += 1;
                }

                // Slow period between bursts
                std.Thread.sleep(100 * std.time.ns_per_us); // 100µs pause
                burst += 1;
            }
            context.producer_done.store(true, .seq_cst);
        }
    }.run, .{&ctx});

    // Consumer
    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(context: *Context) void {
            const total = context.num_bursts * context.items_per_burst;
            var count: u64 = 0;

            while (count < total) {
                if (context.queue.pop()) |_| {
                    count += 1;
                } else {
                    std.atomic.spinLoopHint();
                }
            }
        }
    }.run, .{&ctx});

    producer.join();
    consumer.join();

    std.debug.print("  ✓ Handled {d} bursts of {d} items ({d} total)\n", .{ num_bursts, items_per_burst, total_items });
}
