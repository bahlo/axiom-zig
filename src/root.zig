const std = @import("std");
const Allocator = std.mem.Allocator;
const http = std.http;
const fmt = std.fmt;
const json = std.json;

/// A dataset with metadata.
pub const Dataset = struct { id: []const u8, name: []const u8, description: []const u8, who: []const u8, created: []const u8 };

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
};

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
