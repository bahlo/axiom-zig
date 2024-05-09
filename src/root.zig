const std = @import("std");
const Allocator = std.mem.Allocator;
const http = std.http;
const fmt = std.fmt;
const json = std.json;

/// A dataset with metadata.
pub const Dataset = struct { id: []const u8, name: []const u8, description: []const u8, who: []const u8, created: []const u8 };

/// SDK provides methods to interact with the Axiom API.
pub const SDK = struct {
    allocator: Allocator,
    api_token: []const u8,
    http_client: http.Client,

    /// Create a new SDK.
    fn new(allocator: Allocator, api_token: []const u8) SDK {
        return SDK{ .allocator = allocator, .api_token = api_token, .http_client = http.Client{ .allocator = allocator } };
    }

    /// Deinitialize the SDK.
    fn deinit(self: *SDK) void {
        self.http_client.deinit();
    }

    /// Get all datasets the token has access to.
    /// Caller owns the memory.
    fn getDatasets(self: *SDK) ![]Dataset {
        // TODO: Store base URL in global const or struct
        const url = comptime std.Uri.parse("https://api.axiom.co/v2/datasets") catch unreachable;

        var server_header_buffer: [4096]u8 = undefined; // Is 4kb enough?
        var request = try self.http_client.open(.GET, url, .{
            .server_header_buffer = &server_header_buffer,
        });
        defer request.deinit();

        var authorization_header_buf: [64]u8 = undefined;
        const authorization_header = try fmt.bufPrint(&authorization_header_buf, "Bearer {s}", .{self.api_token});
        request.headers.authorization = .{ .override = authorization_header };

        try request.send();
        try request.wait();

        var body: [512 * 1024]u8 = undefined; // 1mb buf should be enough?
        const content_length = try request.reader().readAll(&body);

        const parsed_datasets = try json.parseFromSlice([]Dataset, self.allocator, body[0..content_length], .{});
        defer parsed_datasets.deinit();

        const datasets = try self.allocator.dupe(Dataset, parsed_datasets.value);

        return datasets;
    }
};

test "SDK.init/deinit" {
    var sdk = SDK.new(std.testing.allocator, "token");
    defer sdk.deinit();
    try std.testing.expectEqual(sdk.api_token, "token");
}

test "getDatasets" {
    const allocator = std.testing.allocator;

    const api_token = try std.process.getEnvVarOwned(allocator, "AXIOM_TOKEN");
    defer allocator.free(api_token);

    var sdk = SDK.new(allocator, api_token);
    defer sdk.deinit();

    const datasets = try sdk.getDatasets();
    defer allocator.free(datasets);

    try std.testing.expect(datasets.len > 0);
    try std.testing.expectEqualStrings("_traces", datasets[0].name);
}
