import _ from 'lodash';
import Redis from 'ioredis';
import { Buffer } from 'buffer';

export class RediFlowList {
	public listName: string;
	protected connection: Redis;
	constructor(listName: string, connection: Redis) {
		this.listName = listName;
		this.connection = connection;
	}
	pushJson(...elements: any[]) {
		return this.push(..._.map(elements, (e) => JSON.stringify(e)));
	}
	push(...elements: (string | number | Buffer)[]) {
		return this.connection.rpush(this.listName, ...elements);
	}
	unshiftJson(...elements: any[]) {
		return this.unshift(..._.map(elements, (e) => JSON.stringify(e)));
	}
	unshift(...elements: (string | number | Buffer)[]) {
		return this.connection.lpush(this.listName, ...elements);
	}
	async popJson(blocking = false, timeOut = 0) {
		return JSON.parse((await this.pop(blocking, timeOut)) ?? '{}');
	}
	async pop(blocking = false, timeOut = 0) {
		if (blocking) {
			const [key, value] = (await this.connection.brpop(this.listName, timeOut)) ?? [];
			return value;
		}
		return this.connection.rpop(this.listName);
	}
	async shiftJson(blocking = false, timeOut = 0) {
		return JSON.parse((await this.shift(blocking, timeOut)) ?? '{}');
	}
	async shift(blocking = false, timeOut = 0) {
		if (blocking) {
			const [key, value] = (await this.connection.blpop(this.listName, timeOut)) ?? [];
			return value;
		}
		return this.connection.lpop(this.listName);
	}
}
