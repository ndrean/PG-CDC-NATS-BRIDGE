const std = @import("std");

/// Build nats.c library using CMake
fn buildNats(b: *std.Build) *std.Build.Step {
    const nats_step = b.step("build-nats", "Build nats.c library using CMake");

    // Check if library already exists
    const nats_lib_path = "libs/nats-install/lib/libnats_static.a";
    const nats_lib_exists = blk: {
        std.fs.cwd().access(nats_lib_path, .{}) catch {
            break :blk false;
        };
        break :blk true;
    };

    if (nats_lib_exists) {
        std.debug.print("nats.c library already built at {s}\n", .{nats_lib_path});
        return nats_step;
    }

    // Create build directory
    const mkdir_cmd = b.addSystemCommand(&.{ "mkdir", "-p", "libs/nats.c/build" });
    nats_step.dependOn(&mkdir_cmd.step);

    // Run CMake configure
    const cmake_cmd = b.addSystemCommand(&.{
        "cmake",
        "-S",
        "libs/nats.c",
        "-B",
        "libs/nats.c/build",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DBUILD_TESTING=OFF",
        "-DNATS_BUILD_STREAMING=OFF",
        "-DNATS_BUILD_WITH_TLS=OFF",
        "-DNATS_BUILD_EXAMPLES=OFF",
        "-DNATS_BUILD_LIB_SHARED=OFF",
        "-DCMAKE_INSTALL_PREFIX=libs/nats-install",
    });
    cmake_cmd.step.dependOn(&mkdir_cmd.step);
    nats_step.dependOn(&cmake_cmd.step);

    // Run CMake build
    const build_cmd = b.addSystemCommand(&.{
        "cmake",
        "--build",
        "libs/nats.c/build",
        "--config",
        "Release",
    });
    build_cmd.step.dependOn(&cmake_cmd.step);
    nats_step.dependOn(&build_cmd.step);

    // Run CMake install
    const install_cmd = b.addSystemCommand(&.{
        "cmake",
        "--install",
        "libs/nats.c/build",
    });
    install_cmd.step.dependOn(&build_cmd.step);
    nats_step.dependOn(&install_cmd.step);

    return nats_step;
}

/// Link vendored libpq static libraries to an executable
fn linkLibpq(exe: *std.Build.Step.Compile, b: *std.Build) void {
    // Check if vendored libpq exists
    const vendored_libpq_exists = blk: {
        std.fs.cwd().access("libs/libpq-install/lib/libpq.a", .{}) catch {
            break :blk false;
        };
        break :blk true;
    };

    if (vendored_libpq_exists) {
        // Use vendored libpq
        exe.addIncludePath(b.path("libs/libpq-install/include"));
        exe.addLibraryPath(b.path("libs/libpq-install/lib"));
        exe.addObjectFile(b.path("libs/libpq-install/lib/libpgcommon.a"));
        exe.addObjectFile(b.path("libs/libpq-install/lib/libpgport.a"));
        exe.addObjectFile(b.path("libs/libpq-install/lib/libpq.a"));
        std.debug.print("Using vendored libpq from libs/libpq-install\n", .{});
    } else {
        // Use system libpq (e.g., in Docker/Debian/Alpine)
        // Add system include paths for PostgreSQL headers
        exe.addSystemIncludePath(.{ .cwd_relative = "/usr/include/postgresql" });
        exe.linkSystemLibrary("pq");
        std.debug.print("Using system libpq\n", .{});
    }

    // Link system dependencies that libpq needs
    exe.linkLibC();
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Build nats.c library if needed
    const nats_build_step = buildNats(b);

    const mod = b.addModule("bridge", .{
        .root_source_file = b.path("src/bridge.zig"),
        .target = target,
    });

    const msgpack = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "bridge",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{
                .{ .name = "bridge", .module = mod },
                .{ .name = "msgpack", .module = msgpack.module("msgpack") },
            },
        }),
    });

    // Ensure nats.c is built before compiling
    exe.step.dependOn(nats_build_step);

    // Link against NATS C library
    exe.addIncludePath(b.path("libs/nats-install/include"));
    exe.addLibraryPath(b.path("libs/nats-install/lib"));
    exe.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));

    // Link vendored libpq
    linkLibpq(exe, b);

    b.installArtifact(exe);

    // A top-level step to build and run the application with `zig build run`
    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // ===== Test executables =====
    // Creates an executable that will run `test` blocks from the module.
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    // A run step that will run the test executable.
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Creates an executable that will run `test` blocks from the executable's
    // root module. Note that test executables only test one module at a time,
    // hence why we have to create two separate ones.
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    // A run step that will run the second test executable.
    const run_exe_tests = b.addRunArtifact(exe_tests);

    // A top level step for running all tests. dependOn can be called multiple
    // times and since the two run steps do not depend on one another, this will
    // make the two of them run in parallel.
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // Clean nats build artifacts
    const clean_nats_step = b.step("clean-nats", "Clean nats.c build artifacts");
    const rm_nats_build = b.addSystemCommand(&.{ "rm", "-rf", "libs/nats.c/build", "libs/nats-install" });
    clean_nats_step.dependOn(&rm_nats_build.step);

    // ===== Commented out test executables - files moved to src/test_files/ =====
    // Uncomment and update paths if needed for testing
    //
    // // NATS test executable
    // const nats_test = b.addExecutable(.{
    //     .name = "nats_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/nats_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //     }),
    // });
    // nats_test.addIncludePath(b.path("libs/nats-install/include"));
    // nats_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // nats_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // nats_test.linkLibC();
    // b.installArtifact(nats_test);
    //
    // const nats_test_step = b.step("nats-test", "Test NATS connection");
    // const nats_test_run = b.addRunArtifact(nats_test);
    // nats_test_run.step.dependOn(b.getInstallStep());
    // nats_test_step.dependOn(&nats_test_run.step);
    //
    // // PostgreSQL test executable
    // const pg_test = b.addExecutable(.{
    //     .name = "pg_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/pg_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(pg_test);
    //
    // const pg_test_step = b.step("pg-test", "Test PostgreSQL connection");
    // const pg_test_run = b.addRunArtifact(pg_test);
    // pg_test_run.step.dependOn(b.getInstallStep());
    // pg_test_step.dependOn(&pg_test_run.step);
    //
    // // CDC Demo executable
    // const cdc_demo = b.addExecutable(.{
    //     .name = "cdc_demo",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_demo.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // cdc_demo.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_demo.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_demo.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // cdc_demo.linkLibC();
    // b.installArtifact(cdc_demo);
    //
    // const cdc_demo_step = b.step("cdc-demo", "Run CDC bridge demo");
    // const cdc_demo_run = b.addRunArtifact(cdc_demo);
    // cdc_demo_run.step.dependOn(b.getInstallStep());
    // cdc_demo_step.dependOn(&cdc_demo_run.step);
    //
    // // CDC Load Test executable
    // const cdc_load_test = b.addExecutable(.{
    //     .name = "cdc_load_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_load_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // // Link NATS
    // cdc_load_test.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_load_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_load_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // // Link vendored libpq
    // linkLibpq(cdc_load_test, b);
    // b.installArtifact(cdc_load_test);
    //
    // const cdc_load_test_step = b.step("load-test", "Run CDC load test");
    // const cdc_load_test_run = b.addRunArtifact(cdc_load_test);
    // cdc_load_test_run.step.dependOn(b.getInstallStep());
    // cdc_load_test_step.dependOn(&cdc_load_test_run.step);
    //
    // // WAL Stream Test executable
    // const wal_stream_test = b.addExecutable(.{
    //     .name = "wal_stream_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/wal_stream_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // // Link vendored libpq for replication protocol
    // linkLibpq(wal_stream_test, b);
    // b.installArtifact(wal_stream_test);
    //
    // const wal_stream_test_step = b.step("wal-stream-test", "Test WAL streaming with libpq");
    // const wal_stream_test_run = b.addRunArtifact(wal_stream_test);
    // wal_stream_test_run.step.dependOn(b.getInstallStep());
    // wal_stream_test_step.dependOn(&wal_stream_test_run.step);

    // // Connection State Test
    // const conn_state_test = b.addExecutable(.{
    //     .name = "connection_state_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/connection_state_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(conn_state_test);
    //
    // const conn_state_test_step = b.step("conn-test", "Test connection state handling");
    // const conn_state_test_run = b.addRunArtifact(conn_state_test);
    // conn_state_test_run.step.dependOn(b.getInstallStep());
    // conn_state_test_step.dependOn(&conn_state_test_run.step);
}
