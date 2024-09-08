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

    fn getCurrentUser(self: *SDK) !Value(User) {
        // TODO: Store base URL in global const or struct
        const url = comptime std.Uri.parse("https://api.axiom.co/v1/user") catch unreachable;

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

        const body = try request.reader().readAllAlloc(self.allocator, 1024 * 1024); // 1mb
        defer self.allocator.free(body);

        std.debug.print("BODY: '{s}'\n", .{body});

        var arena_allocator = std.heap.ArenaAllocator.init(self.allocator);
        const user = try json.parseFromSliceLeaky(User, arena_allocator.allocator(), body, .{
            .allocate = .alloc_always,
        });

        return Value(User){ .value = user, .allocator = arena_allocator };
    }

    /// Get all datasets the token has access to.
    /// Caller owns the memory.
    fn getDatasets(self: *SDK) !Value([]Dataset) {
        // TODO: Store base URL in global const or struct
        const url = comptime std.Uri.parse("https://api.axiom.co/v2/datasets") catch unreachable;

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

        const body = try request.reader().readAllAlloc(self.allocator, 1024 * 1024); // 1mb
        defer self.allocator.free(body);

        var arena_allocator = std.heap.ArenaAllocator.init(self.allocator);
        const datasets = try json.parseFromSliceLeaky([]Dataset, arena_allocator.allocator(), body, .{
            .allocate = .alloc_always,
        });

        return Value([]Dataset){ .value = datasets, .allocator = arena_allocator };
    }

    /// Caller owns the memory.
    fn getDataset(self: *SDK, name: []const u8) !Value(Dataset) {
        // TODO: Store base URL in global const or struct
        const uri_str = try std.fmt.allocPrint(self.allocator, "https://api.axiom.co/v2/datasets/{s}", .{name});
        defer self.allocator.free(uri_str);
        const url = std.Uri.parse(uri_str) catch unreachable;

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

        const body = try request.reader().readAllAlloc(self.allocator, 1024 * 1024); // 1mb
        defer self.allocator.free(body);

        var arena_allocator = std.heap.ArenaAllocator.init(self.allocator);
        const datasets = try json.parseFromSliceLeaky(Dataset, arena_allocator.allocator(), body, .{
            .allocate = .alloc_always,
        });

        return Value(Dataset){ .value = datasets, .allocator = arena_allocator };
    }
};

test "getCurrentUser" {
    const allocator = std.testing.allocator;

    const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
    defer allocator.free(api_token);

    var sdk = SDK.init(allocator, api_token);
    defer sdk.deinit();

    var user_res = try sdk.getCurrentUser();
    defer user_res.deinit();
    const user = user_res.value;

    try std.testing.expectEqualStrings("what", user.name);
}

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
