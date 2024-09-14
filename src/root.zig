const std = @import("std");
const Allocator = std.mem.Allocator;
const http = std.http;
const fmt = std.fmt;
const json = std.json;

/// A dataset with metadata.
pub const Dataset = struct { id: []const u8, name: []const u8, description: []const u8, who: []const u8, created: []const u8 };

/// A user.
pub const User = struct {
    email: []const u8,
    id: []const u8,
    name: []const u8,
    role: Role,
};

/// The role of a user.
pub const Role = struct {
    id: []const u8,
    name: []const u8,
};

/// The status of an ingest request.
pub const IngestStatus = struct {
    ingested: u64,
    failed: u64,
    failures: []Failure,
    processedBytes: u64,
    blocksCreated: u64,
    walLength: u64,
};

/// The failure of an ingest request.
pub const Failure = struct {
    @"error": []const u8,
    timestamp: []const u8,
};

/// Value is a wrapper around a value that is allocated on the heap.
pub fn Value(T: anytype) type {
    return struct {
        value: T,
        allocator: std.heap.ArenaAllocator,

        fn deinit(self: *Value(T)) void {
            self.allocator.deinit();
        }
    };
}

/// The content type for ingest_buffer.
pub const IngestContentType = enum { json, ndjson };

/// The encoding for ingest_buffer.
pub const IngestContentEncoding = enum { identity, gzip };

/// IngestOptions can be passed to ingest requests to set content-type and/or
/// content-encoding headers.
pub const IngestOptions = struct {
    content_type: IngestContentType = .json,
    content_encoding: IngestContentEncoding = .identity,
};

/// SDK provides methods to interact with the Axiom API.
pub const SDK = struct {
    allocator: Allocator,
    api_token: []const u8,
    http_client: http.Client,

    /// Create a new SDK.
    fn init(allocator: Allocator, api_token: []const u8) SDK {
        return SDK{ .allocator = allocator, .api_token = api_token, .http_client = http.Client{ .allocator = allocator } };
    }

    test init {
        var sdk = SDK.init(std.testing.allocator, "token");
        defer sdk.deinit();
        try std.testing.expectEqual(sdk.api_token, "token");
    }

    /// Deinitialize the SDK.
    fn deinit(self: *SDK) void {
        self.http_client.deinit();
    }

    // Get does a get request and parses the (JSON) response.
    // Caller is responsible for calling deinit() on the returned value.
    fn get(self: *SDK, url: std.Uri, T: anytype) !Value(T) {
        var server_header_buffer: [8192]u8 = undefined; // 8kb
        var request = try self.http_client.open(.GET, url, .{
            .server_header_buffer = &server_header_buffer,
        });
        defer request.deinit();

        var authorization_header_buf: [64]u8 = undefined;
        const authorization_header = try fmt.bufPrint(&authorization_header_buf, "Bearer {s}", .{self.api_token});
        request.headers.authorization = .{ .override = authorization_header };

        try request.send();
        try request.wait();

        const body = try request.reader().readAllAlloc(self.allocator, 1024 * 1024); // TODO: Increase max size
        defer self.allocator.free(body);

        var arena_allocator = std.heap.ArenaAllocator.init(self.allocator);
        const value = try json.parseFromSliceLeaky(T, arena_allocator.allocator(), body, .{
            .allocate = .alloc_always,
        });

        return Value(T){ .value = value, .allocator = arena_allocator };
    }

    fn getCurrentUser(self: *SDK) !Value(User) {
        // TODO: Store base URL in global const or struct
        const url = comptime std.Uri.parse("https://api.axiom.co/v2/user") catch unreachable;
        return self.get(url, User);
    }

    /// Get all datasets the token has access to.
    /// Caller owns the memory.
    fn getDatasets(self: *SDK) !Value([]Dataset) {
        // TODO: Store base URL in global const or struct
        const uri = comptime std.Uri.parse("https://api.axiom.co/v2/datasets") catch unreachable;
        return self.get(uri, []Dataset);
    }

    /// Caller owns the memory.
    fn getDataset(self: *SDK, name: []const u8) !Value(Dataset) {
        // TODO: Store base URL in global const or struct
        const uri_str = try std.fmt.allocPrint(self.allocator, "https://api.axiom.co/v2/datasets/{s}", .{name});
        defer self.allocator.free(uri_str);
        const uri = std.Uri.parse(uri_str) catch unreachable;
        return self.get(uri, Dataset);
    }

    /// Caller owns the memory.
    fn ingestBuffer(self: *SDK, dataset: []const u8, buffer: []const u8, opts: IngestOptions) !Value(IngestStatus) {
        // TODO: Store base URL in global const or struct
        const uri_str = try std.fmt.allocPrint(self.allocator, "https://api.axiom.co/v1/datasets/{s}/ingest", .{dataset});
        defer self.allocator.free(uri_str);
        const url = std.Uri.parse(uri_str) catch unreachable;

        var server_header_buffer: [8192]u8 = undefined; // 8kb
        var request = try self.http_client.open(.POST, url, .{
            .server_header_buffer = &server_header_buffer,
        });
        defer request.deinit();

        var authorization_header_buf: [64]u8 = undefined;
        const authorization_header = try fmt.bufPrint(&authorization_header_buf, "Bearer {s}", .{self.api_token});
        request.headers.authorization = .{ .override = authorization_header };
        switch (opts.content_type) {
            .json => {
                request.headers.content_type = .{ .override = "application/json" };
            },
            .ndjson => {
                request.headers.content_type = .{ .override = "application/x-ndjson" };
            },
        }
        switch (opts.content_encoding) {
            .gzip => {
                request.extra_headers = &[_]http.Header{
                    .{
                        .name = "content-encoding",
                        .value = "gzip",
                    },
                };
            },
            .identity => {}, // nop
        }
        request.transfer_encoding = .{ .content_length = buffer.len };

        try request.send();
        var writer = request.writer();
        _ = try writer.writeAll(buffer);
        try request.finish();
        try request.wait();

        const body = try request.reader().readAllAlloc(self.allocator, 1024 * 1024); // 1mb
        defer self.allocator.free(body);

        var arena_allocator = std.heap.ArenaAllocator.init(self.allocator);
        const datasets = try json.parseFromSliceLeaky(IngestStatus, arena_allocator.allocator(), body, .{
            .allocate = .alloc_always,
        });

        return Value(IngestStatus){ .value = datasets, .allocator = arena_allocator };
    }
};

// test "getCurrentUser" {
//     const allocator = std.testing.allocator;
//
//     const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
//     defer allocator.free(api_token);
//
//     var sdk = SDK.init(allocator, api_token);
//     defer sdk.deinit();
//
//     var user_res = try sdk.getCurrentUser();
//     defer user_res.deinit();
//     const user = user_res.value;
//
//     try std.testing.expectEqualStrings("what", user.name);
// }

test "getDatasets" {
    const allocator = std.testing.allocator;

    const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
    defer allocator.free(api_token);

    var sdk = SDK.init(allocator, api_token);
    defer sdk.deinit();

    var datasets_res = try sdk.getDatasets();
    defer datasets_res.deinit();
    const datasets = datasets_res.value;

    try std.testing.expect(datasets.len > 0);
    try std.testing.expectEqualStrings("_traces", datasets[0].name);
}

test "getDataset" {
    const allocator = std.testing.allocator;

    const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
    defer allocator.free(api_token);

    var sdk = SDK.init(allocator, api_token);
    defer sdk.deinit();

    var dataset_res = try sdk.getDataset("axiom.zig");
    defer dataset_res.deinit();
    const dataset = dataset_res.value;

    try std.testing.expectEqualStrings("axiom.zig", dataset.name);
}

test "ingestBuffer" {
    const allocator = std.testing.allocator;

    const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
    defer allocator.free(api_token);

    var sdk = SDK.init(allocator, api_token);
    defer sdk.deinit();

    var ingest_res = try sdk.ingestBuffer("axiom.zig", "[{\"foo\":42}]", .{});
    defer ingest_res.deinit();
    const ingest_status = ingest_res.value;

    try std.testing.expectEqual(1, ingest_status.ingested);
}
