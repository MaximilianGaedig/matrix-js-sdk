/*
Copyright 2026 The Matrix.org Foundation C.I.C.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import { ConnectionError, MatrixError, MatrixEvent, MatrixScheduler, SendTimeoutError } from "../../src";
import { MatrixClient, SEND_EVENT_TIMEOUT_MS } from "../../src/client";

describe("sending an event that gets no answer", () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("should abandon the request and fail with a SendTimeoutError", async () => {
        const client = new MatrixClient({ baseUrl: "https://example.org", userId: "@alice:example.org" });
        const timeout = new AbortController();
        const timeoutSpy = vi.spyOn(AbortSignal, "timeout").mockReturnValue(timeout.signal);
        vi.spyOn(client.http, "authedRequest").mockImplementation(
            (_method, _path, _query, _body, opts) =>
                new Promise((_resolve, reject) => {
                    opts?.abortSignal?.addEventListener("abort", () => reject(new ConnectionError("fetch failed")));
                }),
        );
        const event = new MatrixEvent({ type: "m.room.message", room_id: "!r:example.org", content: { body: "hi" } });
        event.setTxnId("t1");

        const sent = (client as any).sendEventHttpRequest(event, {});
        timeout.abort();

        await expect(sent).rejects.toBeInstanceOf(SendTimeoutError);
        expect(timeoutSpy).toHaveBeenCalledWith(SEND_EVENT_TIMEOUT_MS);
    });

    it("should pass other failures through unchanged", async () => {
        const client = new MatrixClient({ baseUrl: "https://example.org", userId: "@alice:example.org" });
        const forbidden = new MatrixError({ errcode: "M_FORBIDDEN" }, 403);
        vi.spyOn(client.http, "authedRequest").mockRejectedValue(forbidden);
        const event = new MatrixEvent({ type: "m.room.message", room_id: "!r:example.org", content: { body: "hi" } });
        event.setTxnId("t2");

        await expect((client as any).sendEventHttpRequest(event, {})).rejects.toBe(forbidden);
    });
});

describe("MatrixScheduler.RETRY_BACKOFF_RATELIMIT", () => {
    it("should retry a send that timed out", () => {
        expect(MatrixScheduler.RETRY_BACKOFF_RATELIMIT(null, 1, new SendTimeoutError() as any)).toBeGreaterThan(0);
    });

    it("should give up on a send that timed out after the usual number of attempts", () => {
        expect(MatrixScheduler.RETRY_BACKOFF_RATELIMIT(null, 5, new SendTimeoutError() as any)).toBe(-1);
    });

    it("should still not retry other connection errors", () => {
        expect(MatrixScheduler.RETRY_BACKOFF_RATELIMIT(null, 1, new ConnectionError("fetch failed") as any)).toBe(-1);
    });
});
