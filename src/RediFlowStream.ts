import _ from 'lodash';
import Redis from 'ioredis';
import { RediFlowConsumerGroup } from './RediFlowConsumerGroup';

export class RediFlowStream {
	protected connection: Redis;
	protected writeConnection: Redis;
	streamName: string;
	constructor(streamName: string, connection: Redis) {
		this.connection = connection;
		this.writeConnection = connection.duplicate();
		this.streamName = streamName;
	}
	async createConsumerGroup(
		groupName: string,
		consumerName: string,
		{ makeStream, startId }: { makeStream?: boolean; startId: string | '$' | '0' } = {
			startId: '$',
			makeStream: true
		}
	) {
		const consumerGroup = new RediFlowConsumerGroup(groupName, consumerName, this.streamName, this.connection);
		const args: any[] = makeStream ? ['MKSTREAM'] : [];
		try {
			const createdGroup = await this.writeConnection.xgroup('CREATE', this.streamName, groupName, startId, ...args);
		} catch (error) {}
		return consumerGroup;
	}
	getLength() {
		return this.writeConnection.xlen(this.streamName);
	}
	delete(id: string[]) {
		return this.writeConnection.xdel(this.streamName, ...id);
	}
	trimByMaxLength(maxLength: number) {
		return this.writeConnection.xtrim(this.streamName, 'MAXLEN', '~', maxLength);
	}
	trimByMinId(minId: string) {
		return this.writeConnection.xtrim(this.streamName, 'MINID', '=', minId);
	}
	getRange({ start, end, count, reverse }: { start: string; end: string; count?: number; reverse?: boolean }) {
		const func = reverse ? 'xrevrange' : 'xrange';
		if (_.isNil(count)) return this.writeConnection[func](this.streamName, start, end);
		return this.writeConnection[func](this.streamName, start, end, 'COUNT', count);
	}
	addToStream(map: Record<string, any>, id = '*') {
		const properties = _.transform<any, any>(
			map,
			(properties, value, key) => {
				properties.push(key, _.isObjectLike(value) ? JSON.stringify(value) : value);
			},
			[]
		);
		return this.writeConnection.xadd(this.streamName, id, ...properties);
	}
}
