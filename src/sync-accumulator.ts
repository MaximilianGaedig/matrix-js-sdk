/*
Copyright 2017 - 2023 The Matrix.org Foundation C.I.C.

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

/**
 * This is an internal module. See {@link SyncAccumulator} for the public class.
 */

import { logger } from "./logger.ts";
import { deepCopy } from "./utils.ts";
import { MAX_STICKY_DURATION_MS, type IContent, type IUnsigned } from "./models/event.ts";
import { type IRoomSummary } from "./models/room-summary.ts";
import { type EventType } from "./@types/event.ts";
import { UNREAD_THREAD_NOTIFICATIONS } from "./@types/sync.ts";
import { ReceiptAccumulator } from "./receipt-accumulator.ts";
import { type OlmEncryptionInfo } from "./crypto-api/index.ts";
import { type SyncUserProfile } from "./matrix.ts";

interface IOpts {
    /**
     * The ideal maximum number of timeline entries to keep in the sync response.
     * This is best-effort, as clients do not always have a back-pagination token for each event,
     * so it's possible there may be slightly *less* than this value. There will never be more.
     * This cannot be 0 or else it makes it impossible to scroll back in a room.
     * Default: 50.
     */
    maxTimelineEntries?: number;
    /**
     * Whether to use the stable or unstable fields for user profiles.
     */
    profileFieldsStable?: boolean;
}

export interface IMinimalEvent {
    content: IContent;
    type: EventType | string;
    room_id?: string;
    unsigned?: IUnsigned;
}

export interface IEphemeral {
    events: IMinimalEvent[];
}

interface UnreadNotificationCounts {
    highlight_count?: number;
    notification_count?: number;
}

export interface IRoomEvent extends IMinimalEvent {
    event_id: string;
    sender: string;
    origin_server_ts: number;
}

export interface IStateEvent extends IRoomEvent {
    prev_content?: IContent;
    state_key: string;
}

interface IState {
    events: IStateEvent[];
}

export interface ITimeline {
    events: Array<IRoomEvent | IStateEvent>;
    limited?: boolean;
    prev_batch: string | null;
}

type StickyEventFields = {
    msc4354_sticky: { duration_ms: number };
    content: { msc4354_sticky_key?: string };
};

export type IStickyEvent = IRoomEvent & StickyEventFields;

export type IStickyStateEvent = IStateEvent & StickyEventFields;

export interface ISticky {
    events: Array<IStickyEvent | IStickyStateEvent>;
}

export interface IJoinedRoom {
    "summary": IRoomSummary;
    // One of `state` or `state_after` is required.
    "state"?: IState;
    "org.matrix.msc4222.state_after"?: IState; // https://github.com/matrix-org/matrix-spec-proposals/pull/4222
    "msc4354_sticky"?: ISticky; // https://github.com/matrix-org/matrix-spec-proposals/pull/4354
    "timeline": ITimeline;
    "ephemeral": IEphemeral;
    "account_data": IAccountData;
    "unread_notifications": UnreadNotificationCounts;
    "unread_thread_notifications"?: Record<string, UnreadNotificationCounts>;
    "org.matrix.msc3773.unread_thread_notifications"?: Record<string, UnreadNotificationCounts>;
}

export interface IStrippedState {
    content: IContent;
    state_key: string;
    type: EventType | string;
    sender: string;
}

export interface IInviteState {
    events: IStrippedState[];
}

export interface IKnockState {
    events: IStrippedState[];
}

export interface IInvitedRoom {
    invite_state: IInviteState;
}

export interface ILeftRoom {
    // One of `state` or `state_after` is required.
    "state"?: IState;
    "org.matrix.msc4222.state_after"?: IState;
    "timeline": ITimeline;
    "account_data": IAccountData;
}

export interface IKnockedRoom {
    knock_state: IKnockState;
}

export interface IRooms {
    [Category.Join]: Record<string, IJoinedRoom>;
    [Category.Invite]: Record<string, IInvitedRoom>;
    [Category.Leave]: Record<string, ILeftRoom>;
    [Category.Knock]: Record<string, IKnockedRoom>;
}

interface IPresence {
    events: IMinimalEvent[];
}

interface IAccountData {
    events: IMinimalEvent[];
}

/** A to-device message as received from the sync. */
export interface IToDeviceEvent {
    content: IContent;
    sender: string;
    type: string;
}

/**
 * A (possibly decrypted) to-device message after it has been successfully processed by the sdk.
 *
 * If the message was encrypted, the `encryptionInfo` field will contain the encryption information.
 * If the message was sent in clear, this field will be null.
 *
 * The `message` field contains the message `type`, `content`, and `sender` as if the message was sent in clear.
 */
export interface ReceivedToDeviceMessage {
    /** The message type, content, and sender as if the message was sent in clear. */
    message: IToDeviceEvent;
    /**
     * Information about the encryption of the message.
     * Will be null if the message was sent in clear
     */
    encryptionInfo: OlmEncryptionInfo | null;
}

interface IToDevice {
    events: IToDeviceEvent[];
}

export interface IDeviceLists {
    changed?: string[];
    left?: string[];
}

/**
 * The "users" section of the sync update which contains extended profile updates.
 */
export interface UsersUpdate {
    [userId: string]: {
        profile_updates?: SyncUserProfile | null;
    };
}

export interface ISyncResponse {
    "next_batch": string;
    "rooms": IRooms;
    "presence"?: IPresence;
    "account_data": IAccountData;
    "to_device"?: IToDevice;
    "device_lists"?: IDeviceLists;
    "device_one_time_keys_count"?: Record<string, number>;
    "users"?: UsersUpdate;
    "org.matrix.msc4429.users"?: UsersUpdate;
    "device_unused_fallback_key_types"?: string[];
    "org.matrix.msc2732.device_unused_fallback_key_types"?: string[];
}

export enum Category {
    Invite = "invite",
    Leave = "leave",
    Join = "join",
    Knock = "knock",
}

interface TimelineEntry {
    event: IRoomEvent | IStateEvent;
    token: string | null;
}

interface IRoom {
    _currentState: { [eventType: string]: { [stateKey: string]: IStateEvent } };
    _timeline: TimelineEntry[];
    _summary: Partial<IRoomSummary>;
    _accountData: { [eventType: string]: IMinimalEvent };
    _unreadNotifications: Partial<UnreadNotificationCounts>;
    _unreadThreadNotifications?: Record<string, Partial<UnreadNotificationCounts>>;
    _receipts: ReceiptAccumulator;
    _stickyEvents: {
        readonly event: IStickyEvent | IStickyStateEvent;
        /**
         * This is the timestamp at which point it is safe to remove this event from the store.
         * This value is immutable
         */
        readonly expiresTs: number;
    }[];
}

export interface ISyncData {
    nextBatch: string;
    accountData: IMinimalEvent[];
    roomsData: IRooms;
}

type TaggedEvent = IRoomEvent & { _localTs?: number };

function isTaggedEvent(event: IRoomEvent): event is TaggedEvent {
    return "_localTs" in event && event["_localTs"] !== undefined;
}

/**
 * The purpose of this class is to accumulate /sync responses such that a
 * complete "initial" JSON response can be returned which accurately represents
 * the sum total of the /sync responses accumulated to date. It only handles
 * room data: that is, everything under the "rooms" top-level key.
 *
 * This class is used when persisting room data so a complete /sync response can
 * be loaded from disk and incremental syncs can be performed on the server,
 * rather than asking the server to do an initial sync on startup.
 */
export class SyncAccumulator {
    private accountData: Record<string, IMinimalEvent> = {}; // $event_type: Object
    private inviteRooms: Record<string, IInvitedRoom> = {}; // $roomId: { ... sync 'invite' json data ... }
    private knockRooms: Record<string, IKnockedRoom> = {}; // $roomId: { ... sync 'knock' json data ... }
    private joinRooms: { [roomId: string]: IRoom } = {};
    // the /sync token which corresponds to the last time rooms were
    // accumulated. We remember this so that any caller can obtain a
    // coherent /sync response and know at what point they should be
    // streaming from without losing events.
    private nextBatch: string | null = null;

    public constructor(private readonly opts: IOpts = {}) {
        this.opts.maxTimelineEntries = this.opts.maxTimelineEntries || 50;
    }

    public accumulate(syncResponse: ISyncResponse, fromDatabase = false): void {
        this.accumulateRooms(syncResponse, fromDatabase);
        this.accumulateAccountData(syncResponse);
        this.nextBatch = syncResponse.next_batch;
    }

    private accumulateAccountData(syncResponse: ISyncResponse): void {
        if (!syncResponse.account_data || !syncResponse.account_data.events) {
            return;
        }
        // Clobbers based on event type.
        syncResponse.account_data.events.forEach((e) => {
            this.accountData[e.type] = e;
        });
    }

    /**
     * Accumulate incremental /sync room data.
     * @param syncResponse - the complete /sync JSON
     * @param fromDatabase - True if the sync response is one saved to the database
     */
    private accumulateRooms(syncResponse: ISyncResponse, fromDatabase = false): void {
        if (!syncResponse.rooms) {
            return;
        }
        if (syncResponse.rooms.invite) {
            Object.keys(syncResponse.rooms.invite).forEach((roomId) => {
                this.accumulateRoom(roomId, Category.Invite, syncResponse.rooms.invite[roomId], fromDatabase);
            });
        }
        if (syncResponse.rooms.join) {
            Object.keys(syncResponse.rooms.join).forEach((roomId) => {
                this.accumulateRoom(roomId, Category.Join, syncResponse.rooms.join[roomId], fromDatabase);
            });
        }
        if (syncResponse.rooms.leave) {
            Object.keys(syncResponse.rooms.leave).forEach((roomId) => {
                this.accumulateRoom(roomId, Category.Leave, syncResponse.rooms.leave[roomId], fromDatabase);
            });
        }
        if (syncResponse.rooms.knock) {
            Object.keys(syncResponse.rooms.knock).forEach((roomId) => {
                this.accumulateRoom(roomId, Category.Knock, syncResponse.rooms.knock[roomId], fromDatabase);
            });
        }
    }

    private accumulateRoom(roomId: string, category: Category.Invite, data: IInvitedRoom, fromDatabase: boolean): void;
    private accumulateRoom(roomId: string, category: Category.Join, data: IJoinedRoom, fromDatabase: boolean): void;
    private accumulateRoom(roomId: string, category: Category.Leave, data: ILeftRoom, fromDatabase: boolean): void;
    private accumulateRoom(roomId: string, category: Category.Knock, data: IKnockedRoom, fromDatabase: boolean): void;
    private accumulateRoom(roomId: string, category: Category, data: any, fromDatabase = false): void {
        // Valid /sync state transitions
        //       +--------+ <======+            1: Accept an invite
        //   +== | INVITE |        | (5)        2: Leave a room
        //   |   +--------+ =====+ |            3: Join a public room previously
        //   |(1)            (4) | |               left (handle as if new room)
        //   V         (2)       V |            4: Reject an invite
        // +------+ ========> +--------+         5: Invite to a room previously
        // | JOIN |    (3)    | LEAVE* |            left (handle as if new room)
        // +------+ <======== +--------+
        //
        // * equivalent to "no state"
        switch (category) {
            case Category.Invite: // (5)
                if (this.knockRooms[roomId]) {
                    // was previously knock, now invite, need to delete knock state
                    delete this.knockRooms[roomId];
                }
                this.accumulateInviteState(roomId, data as IInvitedRoom);
                break;

            case Category.Knock:
                this.accumulateKnockState(roomId, data as IKnockedRoom);
                break;

            case Category.Join:
                if (this.knockRooms[roomId]) {
                    // delete knock state on join
                    delete this.knockRooms[roomId];
                } else if (this.inviteRooms[roomId]) {
                    // (1)
                    // was previously invite, now join. We expect /sync to give
                    // the entire state and timeline on 'join', so delete previous
                    // invite state
                    delete this.inviteRooms[roomId];
                }
                // (3)
                this.accumulateJoinState(roomId, data as IJoinedRoom, fromDatabase);
                break;

            case Category.Leave:
                if (this.knockRooms[roomId]) {
                    // delete knock state on leave
                    delete this.knockRooms[roomId];
                } else if (this.inviteRooms[roomId]) {
                    // (4)
                    delete this.inviteRooms[roomId];
                } else {
                    // (2)
                    delete this.joinRooms[roomId];
                }
                break;

            default:
                logger.error("Unknown cateogory: ", category);
        }
    }

    private accumulateInviteState(roomId: string, data: IInvitedRoom): void {
        if (!data.invite_state || !data.invite_state.events) {
            // no new data
            return;
        }
        if (!this.inviteRooms[roomId]) {
            this.inviteRooms[roomId] = {
                invite_state: data.invite_state,
            };
            return;
        }
        // accumulate extra keys for invite->invite transitions
        // clobber based on event type / state key
        // We expect invite_state to be small, so just loop over the events
        const currentData = this.inviteRooms[roomId];
        data.invite_state.events.forEach((e) => {
            let hasAdded = false;
            for (let i = 0; i < currentData.invite_state.events.length; i++) {
                const current = currentData.invite_state.events[i];
                if (current.type === e.type && current.state_key == e.state_key) {
                    currentData.invite_state.events[i] = e; // update
                    hasAdded = true;
                }
            }
            if (!hasAdded) {
                currentData.invite_state.events.push(e);
            }
        });
    }

    private accumulateKnockState(roomId: string, data: IKnockedRoom): void {
        if (!data.knock_state || !data.knock_state.events) {
            // no new data
            return;
        }
        if (!this.knockRooms[roomId]) {
            this.knockRooms[roomId] = {
                knock_state: data.knock_state,
            };
            return;
        }
        // accumulate extra keys
        // clobber based on event type / state key
        // We expect knock_state to be small, so just loop over the events
        const currentData = this.knockRooms[roomId];
        data.knock_state.events.forEach((e) => {
            let hasAdded = false;
            for (let i = 0; i < currentData.knock_state.events.length; i++) {
                const current = currentData.knock_state.events[i];
                if (current.type === e.type && current.state_key == e.state_key) {
                    currentData.knock_state.events[i] = e; // update
                    hasAdded = true;
                }
            }
            if (!hasAdded) {
                currentData.knock_state.events.push(e);
            }
        });
    }

    // Accumulate timeline and state events in a room.
    private accumulateJoinState(roomId: string, data: IJoinedRoom, fromDatabase = false): void {
        const now = Date.now();
        // We expect this function to be called a lot (every /sync) so we want
        // this to be fast. /sync stores events in an array but we often want
        // to clobber based on type/state_key. Rather than convert arrays to
        // maps all the time, just keep private maps which contain
        // the actual current accumulated sync state, and array-ify it when
        // getJSON() is called.

        // State resolution:
        // The 'state' key is the delta from the previous sync (or start of time
        // if no token was supplied), to the START of the timeline. To obtain
        // the current state, we need to "roll forward" state by reading the
        // timeline. We want to store the current state so we can drop events
        // out the end of the timeline based on opts.maxTimelineEntries.
        //
        //      'state'     'timeline'     current state
        // |-------x<======================>x
        //          T   I   M   E
        //
        // When getJSON() is called, we 'roll back' the current state by the
        // number of entries in the timeline to work out what 'state' should be.

        // Back-pagination:
        // On an initial /sync, the server provides a back-pagination token for
        // the start of the timeline. When /sync deltas come down, they also
        // include back-pagination tokens for the start of the timeline. This
        // means not all events in the timeline have back-pagination tokens, as
        // it is only the ones at the START of the timeline which have them.
        // In order for us to have a valid timeline (and back-pagination token
        // to match), we need to make sure that when we remove old timeline
        // events, that we roll forward to an event which has a back-pagination
        // token. This means we can't keep a strict sliding-window based on
        // opts.maxTimelineEntries, and we may have a few less. We should never
        // have more though, provided that the /sync limit is less than or equal
        // to opts.maxTimelineEntries.

        if (!this.joinRooms[roomId]) {
            // Create truly empty objects so event types of 'hasOwnProperty' and co
            // don't cause this code to break.
            this.joinRooms[roomId] = {
                _currentState: Object.create(null),
                _timeline: [],
                _accountData: Object.create(null),
                _unreadNotifications: {},
                _unreadThreadNotifications: {},
                _summary: {},
                _receipts: new ReceiptAccumulator(),
                _stickyEvents: [],
            };
        }
        const currentData = this.joinRooms[roomId];

        if (data.account_data && data.account_data.events) {
            // clobber based on type
            data.account_data.events.forEach((e) => {
                currentData._accountData[e.type] = e;
            });
        }

        // these probably clobber, spec is unclear.
        if (data.unread_notifications) {
            currentData._unreadNotifications = data.unread_notifications;
        }
        currentData._unreadThreadNotifications =
            data[UNREAD_THREAD_NOTIFICATIONS.stable!] ?? data[UNREAD_THREAD_NOTIFICATIONS.unstable!] ?? undefined;

        if (data.summary) {
            const HEROES_KEY = "m.heroes";
            const INVITED_COUNT_KEY = "m.invited_member_count";
            const JOINED_COUNT_KEY = "m.joined_member_count";

            const acc = currentData._summary;
            const sum = data.summary;
            acc[HEROES_KEY] = sum[HEROES_KEY] ?? acc[HEROES_KEY];
            acc[JOINED_COUNT_KEY] = sum[JOINED_COUNT_KEY] ?? acc[JOINED_COUNT_KEY];
            acc[INVITED_COUNT_KEY] = sum[INVITED_COUNT_KEY] ?? acc[INVITED_COUNT_KEY];
        }

        // We purposefully do not persist m.typing events.
        // Technically you could refresh a browser before the timer on a
        // typing event is up, so it'll look like you aren't typing when
        // you really still are. However, the alternative is worse. If
        // we do persist typing events, it will look like people are
        // typing forever until someone really does start typing (which
        // will prompt Synapse to send down an actual m.typing event to
        // clobber the one we persisted).

        // Persist the receipts
        currentData._receipts.consumeEphemeralEvents(data.ephemeral?.events);

        // if we got a limited sync, we need to remove all timeline entries or else
        // we will have gaps in the timeline.
        if (data.timeline && data.timeline.limited) {
            currentData._timeline = [];
        }

        // Work out the current state. The deltas need to be applied in the order:
        // - existing state which didn't come down /sync.
        // - State events under the 'state' key.
        // - State events under the 'state_after' key OR state events in the 'timeline' if 'state_after' is not present.
        data.state?.events?.forEach((e) => {
            setState(currentData._currentState, e);
        });
        data["org.matrix.msc4222.state_after"]?.events?.forEach((e) => {
            setState(currentData._currentState, e);
        });
        data.timeline?.events?.forEach((e, index) => {
            if (!data["org.matrix.msc4222.state_after"]) {
                // this nops if 'e' isn't a state event
                setState(currentData._currentState, e);
            }
            // append the event to the timeline. The back-pagination token
            // corresponds to the first event in the timeline
            let transformedEvent: TaggedEvent;
            if (!fromDatabase) {
                transformedEvent = Object.assign({}, e);
                if (transformedEvent.unsigned !== undefined) {
                    transformedEvent.unsigned = Object.assign({}, transformedEvent.unsigned);
                }
                const age = e.unsigned?.age;
                if (age !== undefined) transformedEvent._localTs = Date.now() - age;
            } else {
                transformedEvent = e;
            }

            currentData._timeline.push({
                event: transformedEvent,
                token: index === 0 ? (data.timeline.prev_batch ?? null) : null,
            });
        });

        // Prune out any events in our stores that have since expired, do this before we
        // insert new events.
        currentData._stickyEvents = currentData._stickyEvents.filter(({ expiresTs }) => expiresTs > now);

        // We want this to be fast, so don't worry about duplicate events here. The RoomStickyEventsStore will
        // process these events into the correct mapped order.
        if (data.msc4354_sticky?.events) {
            currentData._stickyEvents = currentData._stickyEvents.concat(
                data.msc4354_sticky.events.map((event) => {
                    // If `duration_ms` exceeds the spec limit of a hour, we cap it.
                    const cappedDuration = Math.min(event.msc4354_sticky.duration_ms, MAX_STICKY_DURATION_MS);
                    // If `origin_server_ts` claims to have been from the future, we still bound it to now.
                    const createdTs = Math.min(event.origin_server_ts, now);
                    return {
                        event,
                        expiresTs: cappedDuration + createdTs,
                    };
                }),
            );
        }

        // attempt to prune the timeline by jumping between events which have
        // pagination tokens.
        if (currentData._timeline.length > this.opts.maxTimelineEntries!) {
            const startIndex = currentData._timeline.length - this.opts.maxTimelineEntries!;
            for (let i = startIndex; i < currentData._timeline.length; i++) {
                if (currentData._timeline[i].token) {
                    // keep all events after this, including this one
                    currentData._timeline = currentData._timeline.slice(i);
                    break;
                }
            }
        }
    }

    /**
     * Return everything under the 'rooms' key from a /sync response which
     * represents all room data that should be stored. This should be paired
     * with the sync token which represents the most recent /sync response
     * provided to accumulate().
     * @param forDatabase - True to generate a sync to be saved to storage
     * @returns An object with a "nextBatch", "roomsData" and "accountData"
     * keys.
     * The "nextBatch" key is a string which represents at what point in the
     * /sync stream the accumulator reached. This token should be used when
     * restarting a /sync stream at startup. Failure to do so can lead to missing
     * events. The "roomsData" key is an Object which represents the entire
     * /sync response from the 'rooms' key onwards. The "accountData" key is
     * a list of raw events which represent global account data.
     */
    public getJSON(forDatabase = false, trim?: SavedSyncTrim): ISyncData {
        const data: IRooms = {
            join: {},
            invite: {},
            knock: {},
            // always empty. This is set by /sync when a room was previously
            // in 'invite' or 'join'. On fresh startup, the client won't know
            // about any previous room being in 'invite' or 'join' so we can
            // just omit mentioning it at all, even if it has previously come
            // down /sync.
            // The notable exception is when a client is kicked or banned:
            // we may want to hold onto that room so the client can clearly see
            // why their room has disappeared. We don't persist it though because
            // it is unclear *when* we can safely remove the room from the DB.
            // Instead, we assume that if you're loading from the DB, you've
            // refreshed the page, which means you've seen the kick/ban already.
            leave: {},
        };
        Object.keys(this.inviteRooms).forEach((roomId) => {
            data.invite[roomId] = this.inviteRooms[roomId];
        });
        Object.keys(this.knockRooms).forEach((roomId) => {
            data.knock[roomId] = this.knockRooms[roomId];
        });
        Object.keys(this.joinRooms).forEach((roomId) => {
            const roomData = this.joinRooms[roomId];
            const roomJson: IJoinedRoom & {
                // We track both `state` and `state_after` for downgrade compatibility
                "state": IState;
                "org.matrix.msc4222.state_after": IState;
                [STATE_TRIMMED_KEY]?: boolean;
            } = {
                "ephemeral": { events: [] },
                "account_data": { events: [] },
                "state": { events: [] },
                "org.matrix.msc4222.state_after": { events: [] },
                "timeline": {
                    events: [],
                    prev_batch: null,
                },
                "unread_notifications": roomData._unreadNotifications,
                "unread_thread_notifications": roomData._unreadThreadNotifications,
                "summary": roomData._summary as IRoomSummary,
                "msc4354_sticky": roomData._stickyEvents?.length
                    ? {
                          events: roomData._stickyEvents.map((e) => e.event),
                      }
                    : undefined,
            };
            // Add account data
            Object.keys(roomData._accountData).forEach((evType) => {
                roomJson.account_data.events.push(roomData._accountData[evType]);
            });

            const receiptEvent = roomData._receipts.buildAccumulatedReceiptEvent(roomId);

            // add only if we have some receipt data
            if (receiptEvent) {
                roomJson.ephemeral.events.push(receiptEvent);
            }

            // Add timeline data. When trimming, the replay gets only the room's latest events; the ones
            // before sit behind a local pagination token (see getCachedTimelineBefore).
            const trimRoom = !!trim && !this.isReplayedInFull(roomId, roomData, trim);
            let entries = roomData._timeline;
            if (trimRoom) {
                if (trim.skipTimelineTypes?.length) {
                    const skip = new Set(trim.skipTimelineTypes);
                    entries = entries.filter((entry) => !skip.has(entry.event.type));
                }
                const start = trimmedTimelineStart(entries, trim.tail);
                if (start > 0) {
                    entries = entries.slice(start);
                }
                // Wherever the replay now starts, the room must be able to reach what came before it. A
                // room whose stored events were all left out (a quiet chat whose only recent events were
                // a bridge's bookkeeping) would otherwise replay nothing and have nothing to paginate
                // from, leaving it stuck showing an empty timeline.
                const first = entries[0] ?? roomData._timeline[roomData._timeline.length - 1];
                if (first) {
                    roomJson.timeline.prev_batch = LOCAL_PAGINATION_PREFIX + first.event.event_id;
                }
            }
            entries.forEach((msgData) => {
                if (!roomJson.timeline.prev_batch) {
                    // the first event we add to the timeline MUST match up to
                    // the prev_batch token.
                    if (!msgData.token) {
                        return; // this shouldn't happen as we prune constantly.
                    }
                    roomJson.timeline.prev_batch = msgData.token;
                }
                roomJson.timeline.events.push(exportTimelineEvent(msgData, forDatabase));
            });

            // Add state data: roll back current state to the start of timeline,
            // by "reverse clobbering" from the end of the timeline to the start.
            // Convert maps back into arrays.
            const rollBackState = Object.create(null);
            for (let i = roomJson.timeline.events.length - 1; i >= 0; i--) {
                const timelineEvent = roomJson.timeline.events[i];
                if (
                    (timelineEvent as IStateEvent).state_key === null ||
                    (timelineEvent as IStateEvent).state_key === undefined
                ) {
                    continue; // not a state event
                }
                // since we're going back in time, we need to use the previous
                // state value else we'll break causality. We don't have the
                // complete previous state event, so we need to create one.
                const prevStateEvent = deepCopy(timelineEvent);
                if (prevStateEvent.unsigned) {
                    if (prevStateEvent.unsigned.prev_content) {
                        prevStateEvent.content = prevStateEvent.unsigned.prev_content;
                    }
                    if (prevStateEvent.unsigned.prev_sender) {
                        prevStateEvent.sender = prevStateEvent.unsigned.prev_sender;
                    }
                }
                setState(rollBackState, prevStateEvent);
            }
            // A replay is built from `state_after` alone, so the rolled-back copy of the same state is
            // only worth producing for storage, where it is kept for downgrade compatibility. Sending
            // it to the client as well doubles the state it has to clone and turn into events.
            const withRolledBackState = forDatabase || !trim;
            Object.keys(roomData._currentState).forEach((evType) => {
                Object.keys(roomData._currentState[evType]).forEach((stateKey) => {
                    let ev = roomData._currentState[evType][stateKey];
                    // Push to both fields to provide downgrade compatibility in the sync accumulator db
                    // the code will prefer `state_after` if it is present
                    roomJson["org.matrix.msc4222.state_after"].events.push(ev);
                    if (!withRolledBackState) return;
                    // Roll the state back to the value at the start of the timeline if it was changed
                    if (rollBackState[evType] && rollBackState[evType][stateKey]) {
                        ev = rollBackState[evType][stateKey];
                    }
                    roomJson.state.events.push(ev);
                });
            });
            if (trimRoom && trim.listStateTypes) {
                const keep = this.listStateFilter(roomId, roomData, roomJson.timeline.events, trim);
                const before = roomJson.state.events.length;
                roomJson.state.events = roomJson.state.events.filter(keep);
                roomJson["org.matrix.msc4222.state_after"].events =
                    roomJson["org.matrix.msc4222.state_after"].events.filter(keep);
                if (roomJson.state.events.length < before) roomJson[STATE_TRIMMED_KEY] = true;
            }
            data.join[roomId] = roomJson;
        });

        // Add account data
        const accData: IMinimalEvent[] = [];
        Object.keys(this.accountData).forEach((evType) => {
            accData.push(this.accountData[evType]);
        });

        return {
            nextBatch: this.nextBatch!,
            roomsData: data,
            accountData: accData,
        };
    }

    /** Whether a trimmed replay still gives this room in full: the room about to be shown, or a favourite. */
    private isReplayedInFull(roomId: string, roomData: IRoom, trim: SavedSyncTrim): boolean {
        if (trim.fullRoomIds?.includes(roomId)) return true;
        const tags = (roomData._accountData["m.tag"]?.content as { tags?: Record<string, unknown> } | undefined)?.tags;
        return !!tags && !!trim.fullRoomTags?.some((tag) => tag in tags);
    }

    /**
     * Which state a trimmed room keeps in memory: the types the room list needs, and the member events of
     * the user, the room's heroes, its DM partner and the senders of the replayed events.
     */
    private listStateFilter(
        roomId: string,
        roomData: IRoom,
        timeline: (IRoomEvent | IStateEvent)[],
        trim: SavedSyncTrim,
    ): (ev: IStateEvent) => boolean {
        const types = new Set(trim.listStateTypes);
        const members = new Set<string>(roomData._summary["m.heroes"] ?? []);
        if (trim.userId) members.add(trim.userId);
        for (const ev of timeline) members.add(ev.sender);
        const direct = this.accountData["m.direct"]?.content as Record<string, unknown> | undefined;
        for (const [userId, rooms] of Object.entries(direct ?? {})) {
            if (Array.isArray(rooms) && rooms.includes(roomId)) members.add(userId);
        }
        return (ev) => (ev.type === "m.room.member" ? members.has(ev.state_key) : types.has(ev.type));
    }

    /** A room's whole stored current state: what a trimmed replay left out of memory (see SavedSyncTrim). */
    public getCachedRoomState(roomId: string): IStateEvent[] | null {
        const state = this.joinRooms[roomId]?._currentState;
        if (!state) return null;
        return Object.values(state).flatMap((byKey) => Object.values(byKey));
    }

    /**
     * The stored timeline of a room before one of its events: what a trimmed replay left out (see
     * {@link SavedSyncTrim}), with the pagination token to continue from after it. Null when the room
     * isn't stored; no events when the event has since been pruned from the store.
     */
    public getCachedTimelineBefore(roomId: string, eventId: string): CachedTimelineChunk | null {
        const timeline = this.joinRooms[roomId]?._timeline;
        if (!timeline?.length) return null;
        const index = timeline.findIndex((e) => e.event.event_id === eventId);
        // Pruned: the stored window now starts later, so continue from its start (duplicates are dropped).
        const before = index > 0 ? timeline.slice(0, index) : [];
        const first = index > 0 ? timeline[0] : timeline.find((e) => e.token);
        return {
            events: before.map((msgData) => exportTimelineEvent(msgData, false)),
            prevBatch: first?.token ?? null,
        };
    }

    public getNextBatchToken(): string {
        return this.nextBatch!;
    }

    public removeEventsFromRoom(roomId: string, eventIds: string[]): void {
        this.joinRooms[roomId]._timeline = this.joinRooms[roomId]._timeline.filter(
            (ev) => !eventIds.includes(ev.event.event_id),
        );
        this.joinRooms[roomId]._stickyEvents = this.joinRooms[roomId]._stickyEvents.filter(
            (ev) => !eventIds.includes(ev.event.event_id),
        );
    }
}

/** How much of the stored sync to replay at startup: see {@link SyncAccumulator#getJSON}. */
export interface SavedSyncTrim {
    /** Each room's latest events to replay (more when needed to include a message). */
    tail: number;
    /** Rooms replayed in full (the one about to be shown). */
    fullRoomIds?: string[];
    /** Rooms with any of these tags (m.tag) are replayed in full too, e.g. favourites. */
    fullRoomTags?: string[];
    /**
     * Keep only these state types (and a few member events) for the other rooms; the rest of their state
     * stays in the store until {@link MatrixClient#loadStoredRoomState}. Unset keeps all state.
     */
    listStateTypes?: string[];
    /** The user's own ID, whose member event every room keeps. */
    userId?: string;
    /**
     * Event types left out of the replayed timeline of a trimmed room. Bookkeeping state a bridge keeps
     * up to date (import progress, connection state) is rewritten constantly and is only ever read as
     * current state, so replaying it wastes the room's few kept events and the memory they take. The
     * state itself is unaffected: list it in {@link listStateTypes} to keep that.
     */
    skipTimelineTypes?: string[];
}

/** Marks a replayed room whose state was trimmed (see {@link SavedSyncTrim.listStateTypes}). */
export const STATE_TRIMMED_KEY = "mx_state_trimmed";

export interface CachedTimelineChunk {
    /** Chronological. */
    events: (IRoomEvent | IStateEvent)[];
    /** Where the server continues before these events. */
    prevBatch: string | null;
}

/** Marks a pagination token as "the stored timeline before this event ID" rather than a server token. */
export const LOCAL_PAGINATION_PREFIX = "mx_stored_before:";

/**
 * Marks a timeline whose history is reached by paging back from the room's latest event: /messages without a
 * `from`. A room shown from a cache can lack a token for what is before its events (sliding sync carries a
 * connection on without describing rooms again), and without one it looked as if it had no history.
 */
export const FROM_LATEST_PAGINATION_TOKEN = "mx_from_latest";

/** Event types a room preview shows: the tail must reach back to one of these. */
const PREVIEW_TYPES = new Set([
    "m.room.message",
    "m.room.encrypted",
    "m.sticker",
    "m.poll.start",
    "org.matrix.msc3381.poll.start",
    "m.call.invite",
]);
const MAX_TAIL_EXTENSION = 10;

function trimmedTimelineStart(timeline: TimelineEntry[], tail: number): number {
    const start = Math.max(0, timeline.length - tail);
    // Reach back a little for a message the room list can preview; a room with none nearby (only
    // membership and setup events) keeps just its tail.
    const limit = Math.max(0, start - MAX_TAIL_EXTENSION);
    for (let i = timeline.length - 1; i >= limit; i--) {
        if (PREVIEW_TYPES.has(timeline[i].event.type)) return Math.min(i, start);
    }
    return start;
}

function exportTimelineEvent(msgData: TimelineEntry, forDatabase: boolean): IRoomEvent | IStateEvent {
    if (forDatabase || !isTaggedEvent(msgData.event)) return msgData.event;
    // This means we have to copy each event, so we can fix it up to
    // set a correct 'age' parameter whilst keeping the local timestamp
    // on our stored event.
    const transformedEvent: (IRoomEvent | IStateEvent) & { _localTs?: number } = Object.assign({}, msgData.event);
    if (transformedEvent.unsigned !== undefined) {
        transformedEvent.unsigned = Object.assign({}, transformedEvent.unsigned);
    }
    delete transformedEvent._localTs;
    transformedEvent.unsigned = transformedEvent.unsigned || {};
    transformedEvent.unsigned.age = Date.now() - msgData.event._localTs!;
    return transformedEvent;
}

function setState(eventMap: Record<string, Record<string, IStateEvent>>, event: IRoomEvent | IStateEvent): void {
    if ((event as IStateEvent).state_key === null || (event as IStateEvent).state_key === undefined || !event.type) {
        return;
    }
    if (!eventMap[event.type]) {
        eventMap[event.type] = Object.create(null);
    }
    eventMap[event.type][(event as IStateEvent).state_key] = event as IStateEvent;
}
