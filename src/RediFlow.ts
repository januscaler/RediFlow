import Redis, { RedisOptions } from 'ioredis';
import { Subject } from 'rxjs';
import { Buffer } from 'buffer';
import { RediFlowList } from './RediFlowList';
import { RediFlowStream } from './RediFlowStream';

/**
 *  My Lib
 * @example
 * dshhbsd
 *
 */
export class RediFlow {
	protected connection: Redis;
	constructor(public options: RedisOptions) {
		this.connection = this.factory();
		this.connection.duplicate().on('message', (channel, message) => {
			this.messageObservable.next({ channel, message });
		});
	}

	createList(listName: string) {
		return new RediFlowList(listName, this.connection.duplicate());
	}

	createStream(streamName: string) {
		return new RediFlowStream(streamName, this.connection.duplicate());
	}

	async setJsonObject(key: string, value: any = {}) {
		await this.connection.set(key, JSON.stringify(value));
	}
	async getJsonObject(key: string) {
		const result = await this.connection.get(key);
		return JSON.parse(result ?? '{}');
	}

	async deleteJsonObject(key: string) {
		return this.connection.del(key);
	}

	protected messageObservable = new Subject<{
		channel: string;
		message: string;
	}>();

	async publish(channel: string, message: string | Buffer) {
		return this.connection.duplicate().publish(channel, message);
	}

	async publishJson(channel: string, message: any) {
		return this.connection.duplicate().publish(channel, JSON.stringify(message));
	}

	async subscribe(channels: string[]) {
		const currentObservable = new Subject<{
			channel: string;
			message: string;
		}>();
		await this.connection.duplicate().subscribe(...channels);
		const subscription = this.messageObservable.subscribe(({ channel, message }) => {
			if (channels.includes(channel)) {
				currentObservable.next({ channel, message });
			}
		});
		const unsubscribe = async () => {
			await this.connection.duplicate().unsubscribe(...channels);
			subscription.unsubscribe();
		};
		return { consumer: currentObservable, unsubscribe };
	}

	async subscribeJson(channels: string[]) {
		const currentObservable = new Subject<{ channel: string; message: any }>();
		await this.connection.duplicate().subscribe(...channels);
		const subscription = this.messageObservable.subscribe(({ channel, message }) => {
			if (channels.includes(channel)) {
				currentObservable.next({ channel, message: JSON.parse(message) });
			}
		});
		const unsubscribe = async () => {
			await this.connection.duplicate().unsubscribe(...channels);
			subscription.unsubscribe();
		};
		return { consumer: currentObservable, unsubscribe };
	}

	factory() {
		return new Redis(this.options);
	}
}
