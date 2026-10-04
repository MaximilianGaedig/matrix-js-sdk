/*
Copyright 2022 The Matrix.org Foundation C.I.C.

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

import type { SyncCryptoCallbacks } from "./common-crypto/CryptoBackend.ts";
import { NotificationCountType, Room, RoomEvent } from "./models/room.ts";
import { logger } from "./logger.ts";
import { promiseMapSeries, sleep } from "./utils.ts";
import { EventTimeline } from "./models/event-timeline.ts";
import { FROM_LATEST_PAGINATION_TOKEN } from "./sync-accumulator.ts";
import { ClientEvent, type IStoredClientOpts, type MatrixClient } from "./client.ts";
import {
    type ISyncStateData,
    SyncState,
    _createAndReEmitRoom,
    type SyncApiOptions,
    defaultClientOpts,
    defaultSyncApiOpts,
    type SetPresence,
    processSyncCryptoChanges,
} from "./sync.ts";
import { type MatrixEvent } from "./models/event.ts";
import {
    type IMinimalEvent,
    type IRoomEvent,
    type IStateEvent,
    type IStickyEvent,
    type IStickyStateEvent,
    type IStrippedState,
    type ISyncResponse,
    type IToDeviceEvent,
} from "./sync-accumulator.ts";
import { MatrixError } from "./http-api/index.ts";
import {
    type Extension,
    ExtensionState,
    type MSC3575RoomData,
    type MSC3575SlidingSyncResponse,
    type SlidingSync,
    SlidingSyncEvent,
    SlidingSyncState,
} from "./sliding-sync.ts";
import { EventType } from "./@types/event.ts";
import { type IPushRules } from "./@types/PushRules.ts";
import { RoomStateEvent } from "./models/room-state.ts";
import { RoomMemberEvent } from "./models/room-member.ts";
import { KnownMembership } from "./@types/membership.ts";

// Number of consecutive failed syncs that will lead to a syncState of ERROR as opposed
// to RECONNECTING. This is needed to inform the client of server issues when the
// keepAlive is successful but the server /sync fails.
const FAILED_SYNC_ERROR_THRESHOLD = 3;

type ExtensionE2EERequest = {
    enabled: boolean;
};

type ExtensionE2EEResponse = Pick<
    ISyncResponse,
    | "device_lists"
    | "device_one_time_keys_count"
    | "device_unused_fallback_key_types"
    | "org.matrix.msc2732.device_unused_fallback_key_types"
>;

/**
 * Collects the encryption-relevant parts of a sliding sync response, which arrive via two separate extensions
 * (`e2ee` and `to_device`), so that they can be passed to the crypto layer in a single call once the whole response
 * has been processed. See {@link SyncCryptoCallbacks.processSyncChanges} for why this matters.
 */
class E2EESyncChangesCollector {
    private toDeviceEvents: IToDeviceEvent[] = [];
    private e2ee?: ExtensionE2EEResponse;
    private hasChanges = false;

    public constructor(
        private readonly client: MatrixClient,
        private readonly cryptoCallbacks?: SyncCryptoCallbacks,
    ) {}

    public onToDeviceEvents(events: IToDeviceEvent[]): void {
        this.toDeviceEvents = events;
        this.hasChanges = true;
    }

    public onE2EEChanges(data: ExtensionE2EEResponse): void {
        this.e2ee = data;
        this.hasChanges = true;
    }

    /**
     * Pass the collected changes to the crypto layer, and emit the resulting to-device messages on the client.
     *
     * A no-op if nothing has been collected since the last flush, so it is safe to call once per extension.
     */
    public async flush(): Promise<void> {
        if (!this.hasChanges) return;
        const toDeviceEvents = this.toDeviceEvents;
        const e2ee = this.e2ee;
        this.toDeviceEvents = [];
        this.e2ee = undefined;
        this.hasChanges = false;

        // Fields omitted from the `e2ee` extension are unchanged since the last response; the crypto layer knows to
        // interpret them that way given `useMsc4186`.
        await processSyncCryptoChanges(this.client, this.cryptoCallbacks, {
            toDeviceEvents,
            deviceLists: e2ee?.device_lists,
            oneTimeKeysCounts: e2ee?.device_one_time_keys_count,
            unusedFallbackKeys:
                e2ee?.device_unused_fallback_key_types ?? e2ee?.["org.matrix.msc2732.device_unused_fallback_key_types"],
            useMsc4186: true,
        });
        this.cryptoCallbacks?.onSyncCompleted({});
    }
}

class ExtensionE2EE implements Extension<ExtensionE2EERequest, ExtensionE2EEResponse> {
    public constructor(
        private readonly crypto: SyncCryptoCallbacks,
        private readonly collector: E2EESyncChangesCollector,
    ) {}

    public name(): string {
        return "e2ee";
    }

    public when(): ExtensionState {
        return ExtensionState.PreProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionE2EERequest> {
        if (isInitial) {
            // In SSS, the `?pos=` contains the stream position for device list updates.
            // If we do not have a `?pos=` (e.g because we forgot it, or because the server
            // invalidated our connection) then we MUST invlaidate all device lists because
            // the server will not tell us the delta. This will then cause UTDs as we will fail
            // to encrypt for new devices. This is an expensive call, so we should
            // really really remember `?pos=` wherever possible.
            logger.log("ExtensionE2EE: invalidating all device lists due to missing 'pos'");
            await this.crypto.markAllTrackedUsersAsDirty();
        }
        return {
            enabled: true, // this is sticky so only send it on the initial request
        };
    }

    public async onResponse(data: ExtensionE2EEResponse): Promise<void> {
        this.collector.onE2EEChanges(data);
    }

    public async onResponseComplete(): Promise<void> {
        await this.collector.flush();
    }
}

type ExtensionToDeviceRequest = {
    since?: string;
    limit?: number;
    enabled?: boolean;
};

type ExtensionToDeviceResponse = {
    events: Required<ISyncResponse>["to_device"]["events"];
    next_batch: string | null;
};

class ExtensionToDevice implements Extension<ExtensionToDeviceRequest, ExtensionToDeviceResponse> {
    private nextBatch: string | null = null;

    public constructor(private readonly collector: E2EESyncChangesCollector) {}

    public name(): string {
        return "to_device";
    }

    public when(): ExtensionState {
        return ExtensionState.PreProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionToDeviceRequest> {
        return {
            since: this.nextBatch !== null ? this.nextBatch : undefined,
            limit: 100,
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionToDeviceResponse): Promise<void> {
        this.collector.onToDeviceEvents(data["events"] || []);
        this.nextBatch = data.next_batch;
    }

    public async onResponseComplete(): Promise<void> {
        await this.collector.flush();
    }
}

type ExtensionAccountDataRequest = {
    enabled: boolean;
};

type ExtensionAccountDataResponse = {
    global: IMinimalEvent[];
    rooms: Record<string, IMinimalEvent[]>;
};

class ExtensionAccountData implements Extension<ExtensionAccountDataRequest, ExtensionAccountDataResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "account_data";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionAccountDataRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionAccountDataResponse): Promise<void> {
        if (data.global && data.global.length > 0) {
            this.processGlobalAccountData(data.global);
        }

        // oxlint-disable-next-line guard-for-in
        for (const roomId in data.rooms) {
            const accountDataEvents = mapEvents(this.client, roomId, data.rooms[roomId]);
            const room = this.client.getRoom(roomId);
            if (!room) {
                logger.warn("got account data for room but room doesn't exist on client:", roomId);
                continue;
            }
            room.addAccountData(accountDataEvents);
            accountDataEvents.forEach((e) => {
                this.client.emit(ClientEvent.Event, e);
            });
        }
    }

    private processGlobalAccountData(globalAccountData: IMinimalEvent[]): void {
        const events = mapEvents(this.client, undefined, globalAccountData);
        const prevEventsMap = events.reduce<Record<string, MatrixEvent | undefined>>((m, c) => {
            m[c.getType()] = this.client.store.getAccountData(c.getType());
            return m;
        }, {});
        this.client.store.storeAccountDataEvents(events);
        events.forEach((accountDataEvent) => {
            // Honour push rules that come down the sync stream but also
            // honour push rules that were previously cached. Base rules
            // will be updated when we receive push rules via getPushRules
            // (see sync) before syncing over the network.
            if (accountDataEvent.getType() === EventType.PushRules) {
                const rules = accountDataEvent.getContent<IPushRules>();
                this.client.setPushRules(rules);
            }
            const prevEvent = prevEventsMap[accountDataEvent.getType()];
            this.client.emit(ClientEvent.AccountData, accountDataEvent, prevEvent);
            return accountDataEvent;
        });
    }
}

type ExtensionTypingRequest = {
    enabled: boolean;
};

type ExtensionTypingResponse = {
    rooms: Record<string, IMinimalEvent>;
};

class ExtensionTyping implements Extension<ExtensionTypingRequest, ExtensionTypingResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "typing";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionTypingRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionTypingResponse): Promise<void> {
        if (!data?.rooms) {
            return;
        }

        // oxlint-disable-next-line guard-for-in
        for (const roomId in data.rooms) {
            processEphemeralEvents(this.client, roomId, [data.rooms[roomId]]);
        }
    }
}

type ExtensionReceiptsRequest = {
    enabled: boolean;
};

type ExtensionReceiptsResponse = {
    rooms: Record<string, IMinimalEvent>;
};

class ExtensionReceipts implements Extension<ExtensionReceiptsRequest, ExtensionReceiptsResponse> {
    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        return "receipts";
    }

    public when(): ExtensionState {
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionReceiptsRequest> {
        return {
            enabled: true,
        };
    }

    public async onResponse(data: ExtensionReceiptsResponse): Promise<void> {
        if (!data?.rooms) {
            return;
        }

        // oxlint-disable-next-line guard-for-in
        for (const roomId in data.rooms) {
            processEphemeralEvents(this.client, roomId, [data.rooms[roomId]]);
        }
    }
}

type ExtensionStickyEventsRequest = {
    enabled: boolean;
    /** Max events per response; the server may return fewer. */
    limit?: number;
    /** The `next_batch` of the previous response. */
    since?: string;
};

type ExtensionStickyEventsResponse = {
    /** Only sent when there were changes. */
    next_batch?: string;
    rooms?: Record<string, { events: Array<IStickyEvent | IStickyStateEvent> }>;
};

/**
 * Delivers sticky events (MSC4354) over sliding sync.
 * https://github.com/matrix-org/matrix-spec-proposals/pull/4480
 *
 * Sticky events expire after a duration instead of living in the timeline forever, and the server
 * re-sends the unexpired ones (e.g. on join) so late joiners still see them.
 *
 * The server sends them for every room matched by a list or subscription, even rooms currently
 * outside the list window. Sticky events already in a room's timeline are excluded here, so
 * `processRoomData` picks those up separately.
 */
class ExtensionStickyEvents implements Extension<ExtensionStickyEventsRequest, ExtensionStickyEventsResponse> {
    private nextBatch?: string;

    public constructor(private readonly client: MatrixClient) {}

    public name(): string {
        // Keeps MSC4354's number, as the extension was originally specified there.
        return "org.matrix.msc4354.sticky_events";
    }

    public when(): ExtensionState {
        // Sticky events are stored on a Room, so the room has to exist first.
        return ExtensionState.PostProcess;
    }

    public async onRequest(isInitial: boolean): Promise<ExtensionStickyEventsRequest> {
        return {
            enabled: true,
            limit: 100,
            // Undefined until the first response, which asks for all unexpired sticky events.
            since: this.nextBatch,
        };
    }

    public async onResponse(data: ExtensionStickyEventsResponse): Promise<void> {
        for (const [roomId, roomData] of Object.entries(data?.rooms ?? {})) {
            const room = this.client.getRoom(roomId);
            if (!room) {
                // Dropping is safe: unexpired sticky events are re-sent once we know the room.
                logger.debug(`Ignoring sticky events for unknown room ${roomId}`);
                continue;
            }
            room._unstable_addStickyEvents(mapEvents(this.client, roomId, roomData.events ?? []));
        }

        // next_batch is only returned when there were changes, and must be echoed back as `since`.
        if (data?.next_batch) {
            this.nextBatch = data.next_batch;
        }
    }
}

/**
 * A copy of SyncApi such that it can be used as a drop-in replacement for sync v2. For the actual
 * sliding sync API, see sliding-sync.ts or the class SlidingSync.
 */
/**
 * What a session keeps of sliding sync for the next one: each room as the server last described it, and
 * the account data. Shown on startup before any request is made.
 */
export interface SlidingSyncSnapshot {
    rooms: Record<string, MSC3575RoomData>;
    accountData: ExtensionAccountDataResponse;
    /**
     * The `pos` of the last response the snapshot has everything of, rooms and account data. The connection
     * carries on from it, so the server sends only what changed since, not every room again.
     */
    pos?: string;
}

/**
 * Where a client keeps its {@link SlidingSyncSnapshot}s. `loadFirst` is what the first screen needs (the rooms
 * at the top of the list, and the account data), read and shown before anything else; `loadRest` is every
 * other room, shown after in small batches so that the first screen is not held up by rooms nobody sees yet.
 */
export interface SlidingSyncCache {
    loadFirst(): Promise<SlidingSyncSnapshot | null>;
    loadRest(): Promise<Record<string, MSC3575RoomData> | null>;
}

/** How many cached rooms are replayed between yields, once the first screen is up. */
const CACHE_REPLAY_BATCH = 50;

export class SlidingSyncSdk {
    private readonly opts: IStoredClientOpts;
    private readonly syncOpts: SyncApiOptions;
    private syncState: SyncState | null = null;
    private syncStateData?: ISyncStateData;
    private lastPos: string | null = null;
    /** The cached snapshot's `pos`, which the connection carries on from once the cache is shown. */
    private resumePos?: string;
    /** Cached rooms not shown yet: shown in batches, or at once when the live sync describes one first. */
    private pendingRest = new Map<string, MSC3575RoomData>();
    /** Settles once the rest of the cache has been read (or there is none to read). */
    private markRestLoaded!: () => void;
    private readonly restLoaded = new Promise<void>((resolve) => (this.markRestLoaded = resolve));
    private failCount = 0;
    private notifEvents: MatrixEvent[] = []; // accumulator of sync events in the current sync response
    private readonly accountData: ExtensionAccountData;
    /** Rooms shown from the cache whose live data has not arrived yet. */
    private readonly fromCache = new Set<string>();
    /** Prepared was announced from the cache: the first live response is not a first sync any more. */
    private preparedFromCache = false;

    public constructor(
        private readonly slidingSync: SlidingSync,
        private readonly client: MatrixClient,
        opts: IStoredClientOpts | undefined,
        syncOpts: SyncApiOptions,
    ) {
        this.opts = defaultClientOpts(opts);
        this.syncOpts = defaultSyncApiOpts(syncOpts);

        if (client.getNotifTimelineSet()) {
            client.reEmitter.reEmit(client.getNotifTimelineSet()!, [RoomEvent.Timeline, RoomEvent.TimelineReset]);
        }

        this.slidingSync.on(SlidingSyncEvent.Lifecycle, this.onLifecycle.bind(this));
        this.slidingSync.on(SlidingSyncEvent.RoomData, this.onRoomData.bind(this));
        // The `e2ee` and `to_device` extensions feed a shared collector, so that the crypto layer sees all the
        // encryption-relevant data from a response in a single call.
        const e2eeCollector = new E2EESyncChangesCollector(this.client, this.syncOpts.cryptoCallbacks);
        this.accountData = new ExtensionAccountData(this.client);
        const extensions: Extension<any, any>[] = [
            new ExtensionToDevice(e2eeCollector),
            this.accountData,
            new ExtensionTyping(this.client),
            new ExtensionReceipts(this.client),
            new ExtensionStickyEvents(this.client),
        ];
        if (this.syncOpts.cryptoCallbacks) {
            extensions.push(new ExtensionE2EE(this.syncOpts.cryptoCallbacks, e2eeCollector));
        }
        extensions.forEach((ext) => {
            this.slidingSync.registerExtension(ext);
        });
    }

    private async onRoomData(roomId: string, roomData: MSC3575RoomData): Promise<void> {
        const waiting = this.pendingRest.get(roomId);
        if (waiting) {
            this.pendingRest.delete(roomId);
            await this.replayRooms({ [roomId]: waiting });
        }
        if (this.fromCache.delete(roomId)) {
            const room = this.client.store.getRoom(roomId);
            // What the cache showed may be some way behind. When what has arrived does not reach back to the
            // earliest cached event, the cached events before it would sit above a gap that no token leads
            // into (cached events come with none): the timeline starts again from what arrived, and the
            // earlier events come back with the history.
            if (room && roomData.limited && roomData.timeline?.length) {
                const earliest = room.getLiveTimeline().getEvents()[0]?.getId();
                if (earliest && !roomData.timeline.some((event) => event.event_id === earliest)) {
                    room.resetLiveTimeline(roomData.prev_batch ?? null, null);
                }
            }
        }
        await this.applyRoomData(roomId, roomData);
    }

    private async applyRoomData(roomId: string, roomData: MSC3575RoomData): Promise<void> {
        let room = this.client.store.getRoom(roomId);
        if (!room) {
            if (!roomData.initial) {
                this.syncOpts.logger.debug(
                    "initial flag not set but no stored room exists for room ",
                    roomId,
                    roomData,
                );
                return;
            }
            room = _createAndReEmitRoom(this.client, roomId, this.opts);
        }
        await this.processRoomData(this.client, room, roomData);
    }

    private onLifecycle(state: SlidingSyncState, resp: MSC3575SlidingSyncResponse | null, err?: Error): void {
        if (err) {
            this.syncOpts.logger.debug("onLifecycle", state, err);
        }
        switch (state) {
            case SlidingSyncState.Complete:
                this.purgeNotifications();
                if (!resp) {
                    break;
                }
                // Element won't stop showing the initial loading spinner unless we fire SyncState.Prepared
                // (which the cache may already have done)
                if (!this.lastPos && !this.preparedFromCache) {
                    this.updateSyncState(SyncState.Prepared, {
                        oldSyncToken: undefined,
                        nextSyncToken: resp.pos,
                        catchingUp: false,
                        fromCache: false,
                    });
                }
                // Conversely, Element won't show the room list unless there is at least 1x SyncState.Syncing
                // so hence for the very first sync we will fire prepared then immediately syncing.
                this.updateSyncState(SyncState.Syncing, {
                    oldSyncToken: this.lastPos!,
                    nextSyncToken: resp.pos,
                    catchingUp: false,
                    fromCache: false,
                });
                this.lastPos = resp.pos;
                break;
            case SlidingSyncState.RequestFinished:
                if (err) {
                    this.failCount += 1;
                    this.updateSyncState(
                        this.failCount > FAILED_SYNC_ERROR_THRESHOLD ? SyncState.Error : SyncState.Reconnecting,
                        {
                            error: new MatrixError(err),
                        },
                    );
                    if (this.shouldAbortSync(new MatrixError(err))) {
                        return; // shouldAbortSync actually stops syncing too so we don't need to do anything.
                    }
                } else {
                    this.failCount = 0;
                    this.syncOpts.logger.debug(
                        `SlidingSyncState.RequestFinished with ${Object.keys(resp?.rooms || []).length} rooms`,
                    );
                }
                break;
        }
    }

    /**
     * Sync rooms the user has left.
     * @returns Resolved when they've been added to the store.
     */
    public async syncLeftRooms(): Promise<Room[]> {
        return []; // TODO
    }

    /**
     * Peek into a room. This will result in the room in question being synced so it
     * is accessible via getRooms(). Live updates for the room will be provided.
     * @param roomId - The room ID to peek into.
     * @returns A promise which resolves once the room has been added to the
     * store.
     */
    public async peek(roomId: string): Promise<Room> {
        return null!; // TODO
    }

    /**
     * Stop polling for updates in the peeked room. NOPs if there is no room being
     * peeked.
     */
    public stopPeeking(): void {
        // TODO
    }

    /**
     * Specify the set_presence value to be used for subsequent calls to the Sync API.
     * @param presence - the presence to specify to set_presence of sync calls
     */
    public setPresence(presence?: SetPresence): void {
        // TODO not possible in sliding sync yet
    }

    /**
     * Returns the current state of this sync object
     * @see MatrixClient#event:"sync"
     */
    public getSyncState(): SyncState | null {
        return this.syncState;
    }

    /**
     * Returns the additional data object associated with
     * the current sync state, or null if there is no
     * such data.
     * Sync errors, if available, are put in the 'error' key of
     * this object.
     */
    public getSyncStateData(): ISyncStateData | null {
        return this.syncStateData ?? null;
    }

    // Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts

    public createRoom(roomId: string): Room {
        // XXX cargoculted from sync.ts
        const { timelineSupport } = this.client;
        const room = new Room(roomId, this.client, this.client.getUserId()!, {
            lazyLoadMembers: this.opts.lazyLoadMembers,
            pendingEventOrdering: this.opts.pendingEventOrdering,
            timelineSupport,
        });
        this.client.reEmitter.reEmit(room, [
            RoomEvent.Name,
            RoomEvent.Redaction,
            RoomEvent.RedactionCancelled,
            RoomEvent.Receipt,
            RoomEvent.Tags,
            RoomEvent.LocalEchoUpdated,
            RoomEvent.AccountData,
            RoomEvent.MyMembership,
            RoomEvent.Timeline,
            RoomEvent.TimelineReset,
        ]);
        this.registerStateListeners(room);
        return room;
    }

    private registerStateListeners(room: Room): void {
        // XXX cargoculted from sync.ts
        // we need to also re-emit room state and room member events, so hook it up
        // to the client now. We need to add a listener for RoomState.members in
        // order to hook them correctly.
        this.client.reEmitter.reEmit(room.currentState, [
            RoomStateEvent.Events,
            RoomStateEvent.Members,
            RoomStateEvent.NewMember,
            RoomStateEvent.Update,
        ]);
        room.currentState.on(RoomStateEvent.NewMember, (event, state, member) => {
            member.user = this.client.getUser(member.userId) ?? undefined;
            this.client.reEmitter.reEmit(member, [
                RoomMemberEvent.Name,
                RoomMemberEvent.Typing,
                RoomMemberEvent.PowerLevel,
                RoomMemberEvent.Membership,
            ]);
        });
    }

    /*
    private deregisterStateListeners(room: Room): void { // XXX cargoculted from sync.ts
        // could do with a better way of achieving this.
        room.currentState.removeAllListeners(RoomStateEvent.Events);
        room.currentState.removeAllListeners(RoomStateEvent.Members);
        room.currentState.removeAllListeners(RoomStateEvent.NewMember);
    } */

    private shouldAbortSync(error: MatrixError): boolean {
        if (error.errcode === "M_UNKNOWN_TOKEN") {
            // The logout already happened, we just need to stop.
            this.syncOpts.logger.warn("Token no longer valid - assuming logout");
            this.stop();
            this.updateSyncState(SyncState.Error, { error });
            return true;
        }
        return false;
    }

    private async processRoomData(client: MatrixClient, room: Room, roomData: MSC3575RoomData): Promise<void> {
        roomData = ensureNameEvent(client, room.roomId, roomData);
        const stateEvents = mapEvents(this.client, room.roomId, roomData.required_state);
        // Prevent events from being decrypted ahead of time
        // this helps large account to speed up faster
        // room::decryptCriticalEvent is in charge of decrypting all the events
        // required for a client to function properly
        let timelineEvents = mapEvents(this.client, room.roomId, roomData.timeline, false);
        const ephemeralEvents: MatrixEvent[] = []; // TODO this.mapSyncEventsFormat(joinObj.ephemeral);

        // TODO: handle threaded / beacon events

        // Whether what arrived overlaps the events the room already has.
        let carriesOn = false;
        if (roomData.limited || roomData.initial) {
            // we should not know about any of these timeline entries if this is a genuinely new room.
            // If we do, then we've effectively done scrollback (e.g requesting timeline_limit: 1 for
            // this room, then timeline_limit: 50).
            const knownEvents = new Set<string>();
            room.getLiveTimeline()
                .getEvents()
                .forEach((e) => {
                    knownEvents.add(e.getId()!);
                });
            // all unknown events BEFORE a known event must be scrollback e.g:
            //       D E   <-- what we know
            // A B C D E F <-- what we just received
            // means:
            // A B C       <-- scrollback
            //       D E   <-- dupes
            //           F <-- new event
            // We bucket events based on if we have seen a known event yet.
            const firstReceived = timelineEvents[0]?.getId();
            const oldEvents: MatrixEvent[] = [];
            const newEvents: MatrixEvent[] = [];
            let seenKnownEvent = false;
            for (let i = timelineEvents.length - 1; i >= 0; i--) {
                const recvEvent = timelineEvents[i];
                if (knownEvents.has(recvEvent.getId()!)) {
                    seenKnownEvent = true;
                    continue; // don't include this event, it's a dupe
                }
                if (seenKnownEvent) {
                    // old -> new
                    oldEvents.push(recvEvent);
                } else {
                    // old -> new
                    newEvents.unshift(recvEvent);
                }
            }
            timelineEvents = newEvents;
            carriesOn = seenKnownEvent;
            if (oldEvents.length > 0) {
                // old events are scrollback, insert them now
                room.addEventsToTimeline(oldEvents, true, false, room.getLiveTimeline(), roomData.prev_batch);
            } else if (
                roomData.prev_batch &&
                firstReceived !== undefined &&
                firstReceived === room.getLiveTimeline().getEvents()[0]?.getId() &&
                [null, FROM_LATEST_PAGINATION_TOKEN].includes(
                    room.getLiveTimeline().getPaginationToken(EventTimeline.BACKWARDS),
                )
            ) {
                // What arrived starts with the earliest event we have, so its token is where the history
                // before ours goes on. Without it the timeline looked complete: a room shown from the cache
                // (whose events come with no token) never loaded anything earlier.
                room.getLiveTimeline().setPaginationToken(roomData.prev_batch, EventTimeline.BACKWARDS);
            }
        }

        const encrypted = room.hasEncryptionStateEvent();
        // we do this first so it's correct when any of the events fire.
        // In an encrypted room the server cannot read the events and counts every one: the client works the count
        // out as it decrypts (fixNotificationCountOnDecryption), and takes the server's only when it is 0, as sync
        // v2 does.
        if (roomData.notification_count != null && (!encrypted || roomData.notification_count === 0)) {
            room.setUnreadNotificationCount(NotificationCountType.Total, roomData.notification_count);
        }

        if (roomData.highlight_count != null) {
            // We track unread notifications ourselves in encrypted rooms, so don't
            // bother setting it here. We trust our calculations better than the
            // server's for this case, and therefore will assume that our non-zero
            // count is accurate.
            if (!encrypted || (encrypted && room.getUnreadNotificationCount(NotificationCountType.Highlight) <= 0)) {
                room.setUnreadNotificationCount(NotificationCountType.Highlight, roomData.highlight_count);
            }
        }
        if (roomData.bump_stamp) {
            room.setBumpStamp(roomData.bump_stamp);
        }

        if (Number.isInteger(roomData.invited_count)) {
            room.currentState.setInvitedMemberCount(roomData.invited_count!);
        }
        if (Number.isInteger(roomData.joined_count)) {
            room.currentState.setJoinedMemberCount(roomData.joined_count!);
        }

        // Invites and knocks come with stripped state at most, and no timeline we may show: for a knock the
        // server may send the room's latest events, which a knock does not give us the right to see.
        const membership = roomData.membership;
        if (roomData.invite_state || membership === KnownMembership.Invite || membership === KnownMembership.Knock) {
            const inviteStateEvents = mapEvents(this.client, room.roomId, roomData.invite_state ?? []);
            await this.injectRoomEvents(room, inviteStateEvents);
            if (membership) room.updateMyMembership(membership);
            if (roomData.initial) {
                room.recalculate();
                this.client.store.storeRoom(room);
                this.client.emit(ClientEvent.Room, room);
            }
            inviteStateEvents.forEach((e) => {
                this.client.emit(ClientEvent.Event, e);
            });
            return;
        }

        if (roomData.limited) {
            const liveTimeline = room.getLiveTimeline();
            if (timelineEvents.length > 0 && liveTimeline.getEvents().length > 0 && !carriesOn) {
                // More happened than was sent, and what was sent does not carry on from what the room has. Glued
                // on, the gap's token would become the token of the room's oldest event: paging back would put
                // the missed events above everything shown, and above the room's creation when it was all shown.
                // As sync v2 does, the live timeline starts again from what arrived, and the old one is kept.
                room.resetLiveTimeline(roomData.prev_batch ?? null, null);
                client.resetNotifTimelineSet();
            } else if (liveTimeline.getEvents().length === 0) {
                // set the back-pagination token. Do this *before* adding any
                // events so that clients can start back-paginating. A timeline that already has events keeps
                // the token of its own first event.
                liveTimeline.setPaginationToken(roomData.prev_batch ?? null, EventTimeline.BACKWARDS);
            }
        }

        /* TODO
        else if (roomData.limited) {

            let limited = true;

            // we've got a limited sync, so we *probably* have a gap in the
            // timeline, so should reset. But we might have been peeking or
            // paginating and already have some of the events, in which
            // case we just want to append any subsequent events to the end
            // of the existing timeline.
            //
            // This is particularly important in the case that we already have
            // *all* of the events in the timeline - in that case, if we reset
            // the timeline, we'll end up with an entirely empty timeline,
            // which we'll try to paginate but not get any new events (which
            // will stop us linking the empty timeline into the chain).
            //
            for (let i = timelineEvents.length - 1; i >= 0; i--) {
                const eventId = timelineEvents[i].getId();
                if (room.getTimelineForEvent(eventId)) {
                    this.syncOpts.logger.debug("Already have event " + eventId + " in limited " +
                        "sync - not resetting");
                    limited = false;

                    // we might still be missing some of the events before i;
                    // we don't want to be adding them to the end of the
                    // timeline because that would put them out of order.
                    timelineEvents.splice(0, i);

                    // XXX: there's a problem here if the skipped part of the
                    // timeline modifies the state set in stateEvents, because
                    // we'll end up using the state from stateEvents rather
                    // than the later state from timelineEvents. We probably
                    // need to wind stateEvents forward over the events we're
                    // skipping.
                    break;
                }
            }

            if (limited) {
                room.resetLiveTimeline(
                    roomData.prev_batch,
                    null, // TODO this.syncOpts.canResetEntireTimeline(room.roomId) ? null : syncEventData.oldSyncToken,
                );

                // We have to assume any gap in any timeline is
                // reason to stop incrementally tracking notifications and
                // reset the timeline.
                this.client.resetNotifTimelineSet();
                this.registerStateListeners(room);
            }
        } */

        await this.injectRoomEvents(room, stateEvents, timelineEvents, roomData.num_live);

        // we deliberately don't add ephemeral events to the timeline
        room.addEphemeralEvents(ephemeralEvents);

        room.setMSC4186SummaryData(roomData.heroes, roomData.joined_count, roomData.invited_count);

        // The MSC4480 extension excludes sticky events already present in the timeline, so we have
        // to pick those up here. See ExtensionStickyEvents for the rest.
        room._unstable_addStickyEvents(timelineEvents.filter((e) => e.unstableStickyInfo !== undefined));

        room.recalculate();
        // local fields must be set before any async calls because call site assumes
        // synchronous execution prior to emitting SlidingSyncState.Complete.
        // The server's membership wins, and is set after recalculate(), which takes it from our member event:
        // a left room is not counted as joined on the way, and a rejected federated invite, which has no member
        // event of ours to send, is left rather than kept as an invite.
        room.updateMyMembership(membership ?? KnownMembership.Join);
        if (roomData.initial) {
            client.store.storeRoom(room);
            client.emit(ClientEvent.Room, room);
        }

        // check if any timeline events should bing and add them to the notifEvents array:
        // we'll purge this once we've fully processed the sync response
        this.addNotifications(timelineEvents);

        const processRoomEvent = async (e: MatrixEvent): Promise<void> => {
            client.emit(ClientEvent.Event, e);
            if (e.isState() && e.getType() == EventType.RoomEncryption && this.syncOpts.cryptoCallbacks) {
                await this.syncOpts.cryptoCallbacks.onCryptoEvent(room, e);
            }
        };

        await promiseMapSeries(stateEvents, processRoomEvent);
        await promiseMapSeries(timelineEvents, processRoomEvent);
        ephemeralEvents.forEach(function (e) {
            client.emit(ClientEvent.Event, e);
        });

        // Decrypt only the last message in all rooms to make sure we can generate a preview
        // And decrypt all events after the recorded read receipt to ensure an accurate
        // notification count
        room.decryptCriticalEvents();
    }

    /**
     * Injects events into a room's model.
     * @param stateEventList - A list of state events. This is the state
     * at the *END* of the timeline list if it is supplied.
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     * @param numLive - the number of events in timelineEventList which just happened,
     * supplied from the server.
     */
    public async injectRoomEvents(
        room: Room,
        stateEventList: MatrixEvent[],
        timelineEventList: MatrixEvent[] = [],
        numLive: number = 0,
    ): Promise<void> {
        // If there are no events in the timeline yet, initialise it with
        // the given state events
        const liveTimeline = room.getLiveTimeline();
        const timelineWasEmpty = liveTimeline.getEvents().length == 0;
        if (timelineWasEmpty) {
            // Passing these events into initialiseState will freeze them, so we need
            // to compute and cache the push actions for them now, otherwise sync dies
            // with an attempt to assign to read only property.
            // XXX: This is pretty horrible and is assuming all sorts of behaviour from
            // these functions that it shouldn't be. We should probably either store the
            // push actions cache elsewhere so we can freeze MatrixEvents, or otherwise
            // find some solution where MatrixEvents are immutable but allow for a cache
            // field.
            for (const ev of stateEventList) {
                this.client.getPushActionsForEvent(ev);
            }
            liveTimeline.initialiseState(stateEventList);
        }

        // If the timeline wasn't empty, we process the state events here: they're
        // defined as updates to the state before the start of the timeline, so this
        // starts to roll the state forward.
        // XXX: That's what we *should* do, but this can happen if we were previously
        // peeking in a room, in which case we obviously do *not* want to add the
        // state events here onto the end of the timeline. Historically, the js-sdk
        // has just set these new state events on the old and new state. This seems
        // very wrong because there could be events in the timeline that diverge the
        // state, in which case this is going to leave things out of sync. However,
        // for now I think it;s best to behave the same as the code has done previously.
        if (!timelineWasEmpty) {
            // XXX: As above, don't do this...
            //room.addLiveEvents(stateEventList || []);
            // Do this instead...
            room.oldState.setStateEvents(stateEventList);
            room.currentState.setStateEvents(stateEventList);
        }

        // the timeline is broken into 'live' events which just happened and normal timeline events
        // which are still to be appended to the end of the live timeline but happened a while ago.
        // The live events are marked as fromCache=false to ensure that downstream components know
        // this is a live event, not historical (from a remote server cache).

        let liveTimelineEvents: MatrixEvent[] = [];
        if (numLive > 0) {
            // last numLive events are live
            liveTimelineEvents = timelineEventList.slice(-1 * numLive);
            // everything else is not live
            timelineEventList = timelineEventList.slice(0, -1 * liveTimelineEvents.length);
        }

        // Execute the timeline events.
        // This also needs to be done before running push rules on the events as they need
        // to be decorated with sender etc.
        await room.addLiveEvents(timelineEventList, {
            fromCache: true,
            addToState: false,
        });
        if (liveTimelineEvents.length > 0) {
            await room.addLiveEvents(liveTimelineEvents, {
                fromCache: false,
                addToState: false,
            });
        }

        room.recalculate();

        // resolve invites now we have set the latest state
        this.resolveInvites(room);
    }

    private resolveInvites(room: Room): void {
        if (!room || !this.opts.resolveInvitesToProfiles) {
            return;
        }
        const client = this.client;
        // For each invited room member we want to give them a displayname/avatar url
        // if they have one (the m.room.member invites don't contain this).
        room.getMembersWithMembership(KnownMembership.Invite).forEach(function (member) {
            if (member.requestedProfileInfo) return;
            member.requestedProfileInfo = true;
            // try to get a cached copy first.
            const user = client.getUser(member.userId);
            let promise: ReturnType<MatrixClient["getProfileInfo"]>;
            if (user) {
                promise = Promise.resolve({
                    avatar_url: user.avatarUrl,
                    displayname: user.displayName,
                });
            } else {
                promise = client.getProfileInfo(member.userId);
            }
            promise.then(
                function (info) {
                    // slightly naughty by doctoring the invite event but this means all
                    // the code paths remain the same between invite/join display name stuff
                    // which is a worthy trade-off for some minor pollution.
                    const inviteEvent = member.events.member!;
                    if (inviteEvent.getContent().membership !== KnownMembership.Invite) {
                        // between resolving and now they have since joined, so don't clobber
                        return;
                    }
                    inviteEvent.getContent().avatar_url = info.avatar_url;
                    inviteEvent.getContent().displayname = info.displayname;
                    // fire listeners
                    member.setMembershipEvent(inviteEvent, room.currentState);
                },
                function (_err) {
                    // OH WELL.
                },
            );
        });
    }

    public retryImmediately(): boolean {
        return true;
    }

    /**
     * Main entry point. Blocks until stop() is called.
     */
    public async sync(): Promise<void> {
        this.syncOpts.logger.debug("Sliding sync init loop");

        //   0) Show what the last session had, before any request: the first screen at once, offline too. The
        //      rest of the cached rooms follow in small batches while the live sync gets going.
        const { rest } = await this.replayCache();

        //   1) We need to get push rules so we can check if events should bing as we get
        //      them from /sync. With the cache's there is nothing to wait for: they are refreshed alongside.
        const fetchPushRules = async (): Promise<boolean> => {
            for (let attempt = 0; !this.client.isGuest(); attempt++) {
                try {
                    this.syncOpts.logger.debug("Getting push rules...");
                    const result = await this.client.getPushRules();
                    this.syncOpts.logger.debug("Got push rules");
                    this.client.pushRules = result;
                    return true;
                } catch (err) {
                    this.syncOpts.logger.error("Getting push rules failed", err);
                    if (this.shouldAbortSync(<MatrixError>err)) {
                        return false;
                    }
                    // Not straight back: offline, that spun as fast as the request could fail.
                    await sleep(Math.min(30_000, 1000 * 2 ** Math.min(attempt, 5)));
                }
            }
            return true;
        };
        if (this.preparedFromCache && this.client.pushRules) {
            void fetchPushRules();
        } else if (!(await fetchPushRules())) {
            return;
        }

        // start syncing
        if (this.resumePos) {
            // Carrying on, the server describes only rooms that changed, and a room it describes before the cache
            // has shown it would have nothing else. The rest of the cache is read first; a room still waiting in
            // it when the server describes it is shown from it then (see onRoomData), so nothing waits for the
            // whole of it.
            await this.restLoaded;
            if (this.resumePos) this.slidingSync.resumeFrom(this.resumePos);
        }
        const started = this.slidingSync.start();
        await rest;
        await started;
    }

    /**
     * Shows the cached first screen and announces the client prepared; returns the replay of the remaining
     * rooms, which runs on in small batches while the first live request is made. It is wrapped: an async
     * function returning the promise itself would make the caller wait for the whole replay.
     */
    private async replayCache(): Promise<{ rest: Promise<void> }> {
        const cache = this.opts.slidingSyncCache;
        if (!cache || this.lastPos) {
            this.markRestLoaded();
            return { rest: Promise.resolve() };
        }
        let first: SlidingSyncSnapshot | null = null;
        try {
            first = await cache.loadFirst();
        } catch (err) {
            this.syncOpts.logger.warn("Sliding sync: could not read the cache", err);
        }
        if (!first) {
            this.markRestLoaded();
            return { rest: Promise.resolve() };
        }

        await this.replayRooms(first.rooms);
        await this.accountData.onResponse(first.accountData);
        this.resumePos = first.pos;
        this.preparedFromCache = true;
        this.updateSyncState(SyncState.Prepared, {
            oldSyncToken: undefined,
            nextSyncToken: undefined,
            catchingUp: false,
            fromCache: true,
        });
        return { rest: this.replayRest(cache, first) };
    }

    private async replayRest(cache: SlidingSyncCache, first: SlidingSyncSnapshot): Promise<void> {
        let rest: Record<string, MSC3575RoomData> | null = null;
        try {
            rest = await cache.loadRest();
        } catch (err) {
            // Without these rooms, carrying on would leave them out until something happens in them.
            this.resumePos = undefined;
            this.syncOpts.logger.warn("Sliding sync: could not read the rest of the cache", err);
        }
        // Rooms the live sync has already described are not replayed over that.
        this.pendingRest = new Map(Object.entries(rest ?? {}).filter(([roomId]) => !this.client.getRoom(roomId)));
        this.markRestLoaded();
        if (!rest) return;
        while (this.pendingRest.size) {
            await new Promise((resolve) => setTimeout(resolve, 0));
            const batch = [...this.pendingRest.entries()].slice(0, CACHE_REPLAY_BATCH);
            for (const [roomId] of batch) this.pendingRest.delete(roomId);
            await this.replayRooms(Object.fromEntries(batch));
        }
        // Their account data (tags, read markers) arrived with the first snapshot, before the rooms existed.
        const late = Object.fromEntries(
            Object.entries(first.accountData.rooms ?? {}).filter(([roomId]) => rest![roomId]),
        );
        if (Object.keys(late).length) await this.accountData.onResponse({ global: [], rooms: late });
    }

    /**
     * A cached room with no token for the history before its events, and not shown back to its creation, pages
     * back from its latest event instead: a connection that carries on does not describe it again, and without
     * a token it looked as if it had no history at all.
     */
    private markHistoryFromLatest(roomId: string): void {
        const timeline = this.client.getRoom(roomId)?.getLiveTimeline();
        if (!timeline || timeline.getPaginationToken(EventTimeline.BACKWARDS)) return;
        if (timeline.getEvents().some((event) => event.getType() === EventType.RoomCreate)) return;
        timeline.setPaginationToken(FROM_LATEST_PAGINATION_TOKEN, EventTimeline.BACKWARDS);
    }

    private async replayRooms(rooms: Record<string, MSC3575RoomData>): Promise<void> {
        for (const [roomId, roomData] of Object.entries(rooms)) {
            if (this.client.getRoom(roomId)) continue;
            this.fromCache.add(roomId);
            try {
                await this.applyRoomData(roomId, { ...roomData, initial: true, num_live: 0 });
                this.markHistoryFromLatest(roomId);
            } catch (err) {
                this.fromCache.delete(roomId);
                this.syncOpts.logger.warn("Sliding sync: could not show cached room", roomId, err);
            }
        }
    }

    /**
     * Stops the sync object from syncing.
     */
    public stop(): void {
        this.syncOpts.logger.debug("SyncApi.stop");
        this.slidingSync.stop();
    }

    /**
     * Sets the sync state and emits an event to say so
     * @param newState - The new state string
     * @param data - Object of additional data to emit in the event
     */
    private updateSyncState(newState: SyncState, data?: ISyncStateData): void {
        const old = this.syncState;
        this.syncState = newState;
        this.syncStateData = data;
        this.client.emit(ClientEvent.Sync, this.syncState, old, data);
    }

    /**
     * Takes a list of timelineEvents and adds and adds to notifEvents
     * as appropriate.
     * This must be called after the room the events belong to has been stored.
     *
     * @param timelineEventList - A list of timeline events. Lower index
     * is earlier in time. Higher index is later.
     */
    private addNotifications(timelineEventList: MatrixEvent[]): void {
        // gather our notifications into this.notifEvents
        if (!this.client.getNotifTimelineSet()) {
            return;
        }
        for (const timelineEvent of timelineEventList) {
            const pushActions = this.client.getPushActionsForEvent(timelineEvent);
            if (pushActions && pushActions.notify && pushActions.tweaks && pushActions.tweaks.highlight) {
                this.notifEvents.push(timelineEvent);
            }
        }
    }

    /**
     * Purge any events in the notifEvents array. Used after a /sync has been complete.
     * This should not be called at a per-room scope (e.g in onRoomData) because otherwise the ordering
     * will be messed up e.g room A gets a bing, room B gets a newer bing, but both in the same /sync
     * response. If we purge at a per-room scope then we could process room B before room A leading to
     * room B appearing earlier in the notifications timeline, even though it has the higher origin_server_ts.
     */
    private purgeNotifications(): void {
        this.notifEvents.sort(function (a, b) {
            return a.getTs() - b.getTs();
        });
        this.notifEvents.forEach((event) => {
            this.client.getNotifTimelineSet()?.addLiveEvent(event, { addToState: false });
        });
        this.notifEvents = [];
    }
}

function ensureNameEvent(client: MatrixClient, roomId: string, roomData: MSC3575RoomData): MSC3575RoomData {
    // make sure m.room.name is in required_state if there is a name, replacing anything previously
    // there if need be. This ensures clients transparently 'calculate' the right room name. Native
    // sliding sync clients should just read the "name" field.
    if (!roomData.name) {
        return roomData;
    }
    for (const stateEvent of roomData.required_state) {
        if (stateEvent.type === EventType.RoomName && stateEvent.state_key === "") {
            stateEvent.content = {
                name: roomData.name,
            };
            return roomData;
        }
    }
    roomData.required_state.push({
        event_id: "$fake-sliding-sync-name-event-" + roomId,
        state_key: "",
        type: EventType.RoomName,
        content: {
            name: roomData.name,
        },
        sender: client.getUserId()!,
        origin_server_ts: new Date().getTime(),
    });
    return roomData;
}

type TaggedEvent = (IStrippedState | IRoomEvent | IStateEvent | IMinimalEvent) & { room_id?: string };

// Helper functions which set up JS SDK structs are below and are identical to the sync v2 counterparts,
// just outside the class.
function mapEvents(client: MatrixClient, roomId: string | undefined, events: object[], decrypt = true): MatrixEvent[] {
    const mapper = client.getEventMapper({ decrypt });
    return (events as TaggedEvent[]).map(function (e) {
        e.room_id = roomId;
        return mapper(e);
    });
}

function processEphemeralEvents(client: MatrixClient, roomId: string, ephEvents: IMinimalEvent[]): void {
    const ephemeralEvents = mapEvents(client, roomId, ephEvents);
    const room = client.getRoom(roomId);
    if (!room) {
        logger.warn("got ephemeral events for room but room doesn't exist on client:", roomId);
        return;
    }
    room.addEphemeralEvents(ephemeralEvents);
    ephemeralEvents.forEach((e) => {
        client.emit(ClientEvent.Event, e);
    });
}
